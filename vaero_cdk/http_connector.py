#
# Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
#
from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional
import requests
from vaero_cdk.api_cursor import APICursor
from vaero_cdk.rate_limiter_steady import rate_limit
from vaero_cdk.rate_limiter_backoff import backoff_handler
from vaero_cdk.exceptions import BackoffException
import time

class HTTPConnector(ABC):
    """
    Abstract base class for a Vaero HTTP Connector.
    """

    def __init__(self, max_calls_per_period: int = 60, limit_period: int = 60, max_retries: int = 6, **kwargs):
        self._session = requests.Session() # session for requests
        self.http_method = "GET" # GET, POST, etc.

        self._incremental_sync = True
        self._cursor = APICursor() # cursor for incremental sync
        self._cursor_location = "temp"

        self._max_calls_per_period = max_calls_per_period # max calls per period
        self._limit_period = limit_period # period in seconds
        self._rate_limit_steady = limit_period / max_calls_per_period
        self._last_send_time = 0 # stores the epoch time in seconds of the last send, for rate limiting
        self._max_retries = max_retries

        print(f"Rate limit steady = {self._rate_limit_steady}")


    def authorize(self) -> bool:
        """
        authorize the configured connector

        :return: True|False on success or failure
        """
        return True

    def has_work(self) -> bool:
        """
        check if the connector has work to do

        :return: True: yes, there is work; False: no
        """
        return True


    #authentication - likely pull out to separate class later
    def get_auth_header(self) -> Mapping[str, Any]:
        """
        Override this method to provide HTTP header authentication token

        :return: The authentication token in {key : value} form
        """
        return {}

    def get_request_headers(self) -> Mapping[str, Any]:
        """
        Override to return request headers
        """
        return {}

    @abstractmethod
    def get_next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Override this method to perform pagination

        The value returned from this method is passed to other methods in this class, such as to set header / query params

        :return: The token for the next page. Returning None means to stop.
        """

    @abstractmethod
    def subpath(self, next_page_token: Mapping[str, Any]) -> str:
        """
        Override this method to return the path to the endpoint

        :return: The URL path for the endpoint, for example return "log" to poll https://api.com/log
        """

    @abstractmethod
    def get_request_params(self, next_page_token: Mapping[str, Any]) -> MutableMapping[str, Any]:
        """
        Override this method to return the request parameters to use on the next call to the API endpoint

        :return: Mapping of {key : value} pairs that define the request params
        """
    
    @abstractmethod
    def parse_response(self, response: requests.Response) -> Iterable[Mapping]:
        """
        Parse the API response into a list

        :return: Iterable version of the raw API response
        """

    def should_retry(self, response: requests.Response) -> bool:
        return response.status_code == 429 or 500 <= response.status_code < 600

    def _get_prepared_request(
        self,
        path: str,
        headers: Mapping = None,
        params: Mapping = None
    ) -> requests.PreparedRequest:

        return self._session.prepare_request(
            requests.Request(
                method = self.http_method,
                url = self._url_base + path, 
                headers = headers,
                params = params
            )
        )

    def _send(self, request: requests.PreparedRequest) -> requests.Response:
        """
        Send request
        """

        print(f"Send request: {request.url}", flush=True) # debug

        self._last_send_time = rate_limit(self._rate_limit_steady, self._last_send_time)

        print(f"Calling _session.send with {request.url}", flush=True)
        response = self._session.send(request) # send API call

        #print(f"Response: url {request.url} status code {response.status_code} with text {response.text}")

        if self.should_retry(response):
            print(f"Failed: Response status code: {response.status_code}")

            # Raise exception triggering backoff
            raise BackoffException(request=request, response=response)
        else:
            # Raise other HTTP errors as exceptions
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                raise exc
        
        return response

    def _send_request(self, request: requests.PreparedRequest) -> requests.Response:
        """
        Backoff and rate limiter wrapper for the _send function
        """

        #response = self._send(request)

        #return response

        # Ensure max tries is at least 1. Backoff library seems to retry for 1 less than max.
        max_tries = self._max_retries
        if max_tries is not None:
            max_tries = max(0, max_tries) + 1

        retry_factor = 5 # retry factor for backoff policy

        return backoff_handler(max_tries = max_tries, factor = retry_factor)(self._send)(request)

    def _update_cursor(self, event_list: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Update and save the cursor

        :return: The new cursor
        """
        return []

    #def _last_cursor(self) -> Mapping[str, Any]:
    #    """
    #    Returns the last (previous) cursor

    #    :return: Last cursor, or empty dictionary if none
    #    """
    #    return []

    #def _set_cursor(self, last_cursor: Mapping[str, Any]):
    #    """
    #    Set the cursor for incremental sync
    #    """

    #    self._session.params.update(last_cursor)
    #    self._current_cursor = last_cursor

    #    return

    def read_stream(self) -> Iterable[Mapping[str, Any]]:
        """
        Poll the API and return the records from the result for 1 page. Call again to get the next page.

        :return: Iterable of the raw API response for 1 page. Returns empty list when done.
        """

        pagination_complete: bool = False

        next_page_token: Optional[Mapping[str, Any]] = None

        while not pagination_complete:

            request = self._get_prepared_request(
                path = self.subpath(next_page_token = next_page_token),
                headers = dict(**self.get_request_headers(), **self.get_auth_header()),
                params = self.get_request_params(next_page_token = next_page_token)
            )

            response = self._send_request(request) # send API call
            yield from self.parse_response(response) # return one page at a time

            next_page_token = self.get_next_page_token(response)
            if not next_page_token:
                pagination_complete = True
        
        yield from [] # return empty list when done
    
    def read(self) -> Iterable[Mapping[str, Any]]:
        """
        Poll the API until all pages are read
        
        :return: Iterable of the raw API response for all pages. Returns empty list when done
        """
        
        if self._incremental_sync:
            self._cursor.load_cursor(self._cursor_location)
            print(f"Cursor: {self._cursor.cursor}") # debug

        event_list = []
        for next_page in self.read_stream():
            event_list.append(next_page)
        
        if self._incremental_sync:
            self._update_cursor(event_list)
            self._cursor.store_cursor(self._cursor_location)

        return event_list
