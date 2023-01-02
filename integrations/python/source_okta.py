from vaero_cdk.http_connector import HTTPConnector, APICursor
from typing import Any, Iterable, Mapping, MutableMapping, Optional
from datetime import datetime, timedelta
import dateutil.parser
import requests
import pytz
import link_header
from urllib.parse import urlparse, parse_qs

class OktaSource(HTTPConnector):
    """
    Class for source connector to Okta
    """

    def __init__(self, interval: int = 0, host: str = "",
                token: str = "", name: str = "okta",
                max_calls_per_period: int = 60, limit_period: int = 60, max_retries: int = 6):
        super().__init__(max_calls_per_period, limit_period, max_retries)

        #print(f"{interval}, {rate_limit}, {host}, {token}, {name}")

        self._url_base = host
        self._okta_token = token
        self._name = name
        self._cursor_location = f"{name}_cursor"

    def authorize(self) -> bool:
        return True

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"Authorization" : f"SSWS {self._okta_token}"}

    def get_next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:

        #print(f"Response Header: {response.headers}")

        next_token = None

        #print(f"Response.json = {response.json()}")

        # Okta returns a link in the HTTP header (rel = next) that includes the after parameter for pagination.
        # Okta System Log API will always return a next link in the polling queries. Therefore, check
        # if the response is empty to determine if we set the next_token or not. If response is empty, then stop.
        # See https://developer.okta.com/docs/reference/core-okta-api/#link-header
        # See also https://developer.okta.com/docs/reference/api/system-log/#request-parameters 
        if self.any_event_post_cursor(response):
            print(f"Any event post cursor = true")
            next_links = link_header.parse(response.headers["link"]).links_by_attr_pairs([('rel', 'next')])
            if next_links:
                parsed_result = urlparse(next_links[0].href)
                #print(f"Parsed result = {parsed_result}")
                query_dict = parse_qs(parsed_result.query)
                #print(f"Query dict: {query_dict}")
                raw_token = query_dict.get("after") # returned as a list of values
                if raw_token:
                    next_token = {"after" : raw_token[0]}
                    print(f"Next token = {next_token}")
                    print(f"{raw_token[0]}")

        return next_token

    def subpath(self, next_page_token: Mapping[str, Any]) -> str:
        return "api/v1/logs"

    def get_request_params(self, next_page_token: Mapping[str, Any]) -> MutableMapping[str, Any]:
        req_params = {"sortOrder" : "ASCENDING"}

        #req_params.update({"limit" : 10}) # debug

        # Set next page token (may be None)
        if next_page_token:
            req_params.update(next_page_token)

        # Set cursor
        if self._cursor.cursor:
            req_params.update(self._cursor.cursor)
        else:
            # Set default cursor. Okta defaults to 7 days in the past, so need to set "since" parameter
            # to go further back. Make "since" 91 days ago because Okta only stores events for 90 days.
            default_since = datetime.now() - timedelta(days = 91)
            default_cursor = {"since" : default_since.isoformat()}
            req_params.update(default_cursor)

        print (f"Request params: {req_params}")

        return req_params
    
    def parse_response(self, response: requests.Response) -> Iterable[Mapping]:

        # Okta API returns events that occurred after the cursor (i.e., up to a second
        # after the "since" time), so we have to filter the events here to delete events
        # that have a "published" timestamp prior to the cursor
        cursor_iso = datetime.min.replace(tzinfo=pytz.UTC)
        if self._cursor.cursor.get("since"):
            cursor_iso = dateutil.parser.parse(self._cursor.cursor.get("since")) # cursor is in iso format
        event_list = [event for event in response.json() if dateutil.parser.parse(event["published"]) >= cursor_iso]
        
        print(f"Parsed {len(event_list)} events") # debug

        """
        print("RESPONSE")
        for r in response.json():
            print(f"{r['published']} and {r['actor']}")

        print("Filtered list")
        for e in event_list:
            print(f"{e['published']} and {e['actor']}")
        """

        return event_list

    
    def _update_cursor(self, event_list: Mapping[str, Any]) -> Mapping[str, Any]:

        # Okta API's option to return ascending order by published date doesn't work and
        # the events are not necessarily in ascending order by published date, so we must
        # iterate over all events and find the max published date

        if event_list:
            last_time = datetime.min.replace(tzinfo=pytz.UTC)
            for event in event_list:
                event_time = dateutil.parser.parse(event["published"])
                last_time = last_time if last_time > event_time else event_time

            last_time += timedelta(milliseconds = 1) # add 1 millisecond to increment cursor

            self._cursor.cursor = {"since" : last_time.isoformat()}

        return self._cursor.cursor

    def all_events_post_cursor(self, response: requests.Response) -> Iterable[Mapping]:
        """
        Return all events with timestamp greater than or equal to the cursor
        """

        cursor_iso = dateutil.parser.parse(self._cursor.cursor.get("since")) if self._cursor.cursor.get("since") else datetime.min.replace(tzinfo=pytz.UTC)
        event_list = [event for event in response.json() if dateutil.parser.parse(event["published"]) >= cursor_iso]

        return event_list
    
    def any_event_post_cursor(self, response: requests.Response) -> bool:
        """
        Return true if any event has a timestamp great than or equal to the cursor, otherwise return false
        """

        cursor_iso = dateutil.parser.parse(self._cursor.cursor.get("since")) if self._cursor.cursor.get("since") else datetime.min.replace(tzinfo=pytz.UTC)
        for event in response.json():
            if dateutil.parser.parse(event["published"]) >= cursor_iso:
                return True
        
        return False


