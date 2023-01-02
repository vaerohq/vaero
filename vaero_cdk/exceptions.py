import requests

class BackoffException(requests.exceptions.HTTPError):
    
    def __init__(self, request: requests.PreparedRequest, response: requests.Response):
        error_message = f"Backoff algorithm exceeded max retries, giving up. Request URL: {request.url}, Response Code: {response.status_code}, Response Content: {response.content}"
        super().__init__(error_message, request = request, response = response)