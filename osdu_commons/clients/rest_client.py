import logging
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)


class RestClient:
    TIMEOUT = 10

    def __init__(self, base_url, timeout_seconds=None):
        self._base_url = base_url if base_url.endswith('/') else f'{base_url}/'
        self._timeout_seconds = self.TIMEOUT if timeout_seconds is None else timeout_seconds

    def post(self, json, path=None, params=None, headers=None):
        response = requests.post(
            url=self._make_url(path=path),
            json=json,
            params=params,
            headers=headers,
            timeout=self._timeout_seconds
        )

        self._check_response(response)
        return response

    @staticmethod
    def _check_response(response):
        status_code = response.status_code
        if status_code // 100 < 4:
            return
        logger.error(f'While sending:\n{response.request.body}\nto {response.request.url} got:\n'
                     f'{status_code} {response.text}')

        if status_code == 404:
            raise HttpNotFoundException(response)
        elif status_code // 100 == 4:
            raise HttpClientException(response)
        elif status_code // 100 == 5:
            raise HttpServerException(response)
        else:
            logger.debug(f'Got unrecognized response {response.status_code}: {response.text}')
            raise HttpUnrecognizedException(response)

    def _make_url(self, path):
        return urljoin(self._base_url, '' if path is None else path)


class HttpException(Exception):
    def __init__(self, response):
        self.response = response

        super().__init__(
            f'{response.request.url}: {self.http_status}, {self.response_text}'
        )

    @property
    def http_status(self):
        return self.response.status_code

    @property
    def response_text(self):
        return self.response.text


class HttpClientException(HttpException):
    pass


class HttpNotFoundException(HttpClientException):
    pass


class HttpServerException(HttpException):
    pass


class HttpUnrecognizedException(HttpException):
    pass
