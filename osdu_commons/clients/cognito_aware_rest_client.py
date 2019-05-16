from osdu_commons.clients.rest_client import RestClient


class CognitoAwareRestClient(RestClient):

    def __init__(self, base_url: str, cognito_headers: dict = None, timeout_seconds=None):
        super().__init__(base_url, timeout_seconds)
        self._cognito_headers = cognito_headers if cognito_headers is not None else {}

    def post(self, *args, **kwargs):
        headers = kwargs.get('headers', {})
        kwargs['headers'] = {**headers, **self._cognito_headers}
        return super().post(*args, **kwargs)
