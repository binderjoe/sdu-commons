import logging
from functools import wraps

import requests
import urllib3

from osdu_commons.clients.rest_client import HttpException

from retrying import retry

logger = logging.getLogger(__name__)

__all__ = [
    'osdu_retry',
]

MAX_RETRIES = 5
WAIT_FIXED = 1000
WAIT_RANDOM_MIN = 0
WAIT_RANDOM_MAX = 1000


def osdu_retry(
        max_retries=MAX_RETRIES,
        wait_fixed=WAIT_FIXED,
        wait_random_min=WAIT_RANDOM_MIN,
        wait_random_max=WAIT_RANDOM_MAX,
):
    def retry_on_exception(e):
        is_timeout = isinstance(e, (requests.exceptions.Timeout, urllib3.exceptions.ReadTimeoutError))
        is_http_exception = isinstance(e, (requests.exceptions.HTTPError, HttpException))
        is_server_error = is_http_exception and e.response.status_code // 100 == 5
        should_be_retried = is_timeout or is_server_error
        if should_be_retried:
            logger.info(f'Retrying request due to the {e.__class__.__name__}')
        else:
            logger.warning(f'Not retrying request because exception is of type {e.__class__.__name__}')
        return should_be_retried

    def decorator(f):
        @wraps(f)
        @retry(
            retry_on_exception=retry_on_exception,
            stop_max_attempt_number=max_retries,
            wait_fixed=wait_fixed,
            wait_random_min=wait_random_min,
            wait_random_max=wait_random_max,
        )
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapper

    return decorator
