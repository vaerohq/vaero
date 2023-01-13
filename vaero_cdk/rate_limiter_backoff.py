#
# Copyright © 2023 Vaero Inc. (https://www.vaero.co/)
#
import backoff
import sys
from typing import Optional
from requests import codes, exceptions
from vaero_cdk.exceptions import BackoffException

BACKOFF_EXCEPTIONS = (
    BackoffException,
    exceptions.ConnectTimeout,
    exceptions.ReadTimeout,
    exceptions.ConnectionError,
    exceptions.ChunkedEncodingError,
)

def backoff_handler(max_tries: Optional[int], factor: float, **kwargs):
    def on_backoff(details):
        _, exc, _ = sys.exc_info()
        if exc.response != None:
            print(f"Status code: {exc.response.status_code}, Response Content: {exc.response.content}")

        print(f"Backoff from error after {details['tries']} tries. Retrying in {details['wait']} seconds.")
    
    def should_give_up(exc):
        # Give up if there is a 400-something status error that is not 429 (Too Many Requests)
        give_up = exc.response is not None and exc.response.status_code != codes.too_many_requests and 400 <= exc.response.status_code < 500
        if give_up:
            print(f"Giving up with HTTP status: {exc.response.status_code}")
        return give_up
    
    return backoff.on_exception(
        backoff.expo,
        BACKOFF_EXCEPTIONS,
        jitter = None,
        on_backoff = on_backoff,
        giveup = should_give_up,
        max_tries = max_tries,
        factor = factor,
        **kwargs
        ) 