from vaero_cdk.http_connector import HTTPConnector, APICursor

from typing import Any, Iterable, Mapping, MutableMapping, Optional

from datetime import date, datetime, timedelta
import dateutil.parser
from dateutil.relativedelta import relativedelta

import requests
from requests.adapters import HTTPAdapter, Retry

import pytz
import link_header
import time
from urllib.parse import urlparse, parse_qs
import jwt
from calendar import timegm


class GoogleWorkspaceSource(HTTPConnector):
    """
    source connector to Google Workspace / GSuite
    """

    _url_base = 'https://admin.googleapis.com/admin/reports/'
    SCOPES = ['https://www.googleapis.com/auth/admin.reports.audit.readonly',
              'https://www.googleapis.com/auth/admin.reports.usage.readonly']

    # reporting lags, calculated in seconds, estimated as per
    # https://support.google.com/a/answer/7061566
    _reporting_lags = {
        'access_transparency': 600,
        'admin': 600,
        'mobile': 600,
        'login': 600,
        'saml': 600,
        'ldap': 600
    }

    def _get_six_months_ago_epoch(self):
        six_months_ago = date.today() + relativedelta(months=-6, days=-1)
        return timegm(six_months_ago.timetuple())

    def _get_default_cursor():
        # the default cursor is set via the various data retention windows here
        # https://support.google.com/a/answer/7061566

        six_months_ago = _get_six_months_ago_epoch()

        default = {
            'access_transparency': six_months_ago,
            'admin': six_months_ago,
            'context_aware_access': six_months_ago,
            'mobile': six_months_ago,
            'login': six_months_ago,
            'saml': six_months_ago,
            'user_accounts': six_months_ago
        }

        return default



    def __init__(self, api_token: str, **kwargs):
        super().__init__(**kwargs)

        # cursor:  { app_name, seconds since 1970 }
        self._cursor_location = "google_workspace_cursor"
        self._secrets = api_token

        self._token = None
        self._token_expiry = None

        # TODO elh: pull from external config
        self._configured_apps = ['admin', 'login', 'saml', 'user_accounts']

        # iterate through configured apps
        self._configured_app_idx = 0

        # the init time: create a constant right query interval.  utc.
        self._init_time = time.time()


    def has_work(self) -> bool:
        """
        checks if there's work

        due to reporting lag, we need time.now - lag > last run time

        :return: True|False on yes/no
        """

        for app in self._configured_apps:
            if app not in self._cursor._cursor.keys():
                return True

            if self._init_time - self._reporting_lags.get(app, 600) > self._cursor[app]:
                return True

        return False


    def authorize(self) -> bool:
        """
        get a bearer token using service account authentication

        TODO
        * expand to optionally use pure oauth, not service_account authentication

        NB: this memoizes the calls and assumes that anything earlier than 20 minutes of expiry is fine
            this will probably fall over soon

        :return: True|False on success/failure
        """

        if self._token_expiry is not None:
            if self._expiry - time.time() > 20 * 60 + 1:
                return True


        # verify config correct
        # we need: an email for the user; and 3 fields from the json key file: type='service_account', private_key, client_email
        all_keys = True
        klass = self.__class__.__name__
        if '@' not in self._secrets.get('email', ''):
            all_keys = False
            print('ERROR: %s: missing config key email' % klass, file=sys.stderr)

        auth_file = self._secrets.get('auth_file', {})
        missing_keys = [k for k in ['client_email', 'private_key', 'type'] if not k in auth_file.keys()]
        if len(missing_keys) > 0:
            all_keys = False
            print('ERROR: %s: missing required config key(s) auth_file.%s' % (klass, missing_keys), file=sys.stderr)

        if auth_file.get('type', '') != 'service_account':
            print('ERROR: %s: incorrect config auth_file.type:%s' % (klass, auth_file.get('type', '')), file=sys.stderr)
            all_keys = False

        if not all_keys:
            raise RuntimeError('missing or incorrect config')

        # run jwt to get a time-limited bearer bond
        # https://developers.google.com/identity/protocols/oauth2/service-account#httprest
        seconds = 60*60 - 1
        expiry = time.time()
        payload = {
            'iss': auth_file['client_email'],
            'scope' : ' '.join(self.SCOPES),
            'aud' : 'https://www.googleapis.com/oauth2/v4/token',
            'exp' : timegm(datetime.utcnow().utctimetuple()) + seconds,
            'iat' : timegm(datetime.utcnow().utctimetuple()),
            'sub' : self._secrets['email']
        }

        encoded = jwt.encode(payload, auth_file['private_key'], algorithm='RS256')

        params = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': encoded
        }
        url = 'https://www.googleapis.com/oauth2/v4/token'

        sess = requests.Session()
        retries = Retry(total=3, backoff_factor=0.2)
        sess.mount('https://', HTTPAdapter(max_retries=retries))

        r = sess.post(url, params=params)
        if r.status_code != 200:
            return False

        j = r.json()
        self._token = j['access_token']
        self._token_expiry = expiry + j['expires_in']

        return True


    def get_auth_header(self) -> Mapping[str, Any]:
        return {
            'Authorization' : f'Bearer {self._token}',
            'Accept' : 'application/json'
        }


    def get_next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # we have to iterate through urls for the different so-called applications
        # so sometimes we have no next page token but need to be called anyway
        # the driver uses None to imply done, so set a flag

        next_token = None
        js = response.json()
        tok = js.get('nextPageToken', None)

        # update cursor for an individual app
        if tok is None:
            app = self._configured_apps[ self._configured_app_idx ]
            self._cursor._cursor[ app ] = self._init_time - self._reporting_lags.get(app, 600)

            if self._configured_app_idx < len(self._configured_apps) - 1:
                tok = '__FLAG__'
                self._configured_app_idx += 1

        return tok


    def subpath(self, next_page_token: Mapping[str, Any]) -> str:
        # docs:  https://developers.google.com/admin-sdk/reports/v1/guides/manage-audit-login
        #
        # a url typically looks like:
        # https://admin.googleapis.com/admin/reports/v1/activity/users/all/applications/login?maxResults=10&alt=json


        # activities of interest
        #   https://developers.google.com/admin-sdk/reports/reference/rest/v1/activities
        #   enum: https://developers.google.com/admin-sdk/reports/reference/rest/v1/activities/list#ApplicationName
        #   access transparency, admin console, Context-aware access, Devices, Login, SAML, User Accounts
        #     if use gcp: google cloud?
        # default_application_names = ['access_transparency', 'admin', 'context_aware_access', 'mobile', 'login', 'saml', 'user_accounts']
        # default_application_names.append('gcp')

        app_name = self._configured_apps[ self._configured_app_idx ]

        return 'v1/activity/users/all/applications/%s' % app_name


    def get_request_params(self, next_page_token: Mapping[str, Any]) -> MutableMapping[str, Any]:
        if self._cursor is None:
            self._cursor = _get_default_cursor()

        # cursor: if someone were to add an app to an already-built cursor, the app
        #         will not be present.  init to 6 months ago
        app = self._configured_apps[ self._configured_app_idx ]
        if app not in self._cursor._cursor.keys():
            self._cursor._cursor[app] = self._get_six_months_ago_epoch()


        # calculate interval [start time, end time), both in utc (format: RFC 3339)
        #   where endTime is offset from now via reporting lag https://support.google.com/a/answer/7061566
        #   assume there is a window or has_work (above) would have failed

        start_time = self._cursor._cursor[app]
        start_time = datetime.utcfromtimestamp(start_time).isoformat()

        end_time = self._init_time - self._reporting_lags.get(app, 600)
        end_time = datetime.utcfromtimestamp(end_time).isoformat()

        # elh: TODO / bug?
        # utc should have a Z on the end.  I'm not sure why the above doesn't?
        start_time += 'Z'
        end_time += 'Z'

        params = {
            'maxResults': 20,
            'startTime': start_time,
            'endTime': end_time
        }

        # the flag value means we've exhausted one of the configured app types
        if next_page_token and next_page_token != '__FLAG__':
            params.update({'pageToken': next_page_token})

        return params


    def parse_response(self, response: requests.Response) -> Iterable[Mapping]:

        js = response.json()
        page_token = js.get('nextPageToken', None)
        event_list = js.get('items', [])

        return event_list


    def _update_cursor(self, event_list: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        unused -- cursor update calculations are elsewhere
        """

        None

