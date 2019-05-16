from botocore.exceptions import ClientError
from retrying import retry

INFINITE_RETRY = 10 ** 6
DEFAULT_WAIT_TIME = 2 * 60 * 1000  # 2 minutes


THROTTLING_ERROR_CODES = [
    'ProvisionedThroughputExceededException',
    'ThrottlingException'
]


def boto_throttle(max_sleep, max_retries):
    def is_boto_throttle(exception):
        return type(exception) is ClientError and exception.response['Error']['Code'] in THROTTLING_ERROR_CODES

    return retry(
        stop_max_attempt_number=max_retries + 1,
        wait_exponential_multiplier=2,
        wait_exponential_max=max_sleep,
        wait_jitter_max=2,
        retry_on_exception=is_boto_throttle
    )


def throttle_exception(exceptions, max_sleep=DEFAULT_WAIT_TIME, max_retries=INFINITE_RETRY):
    def retry_on_exception(exception):
        return type(exception) in exceptions

    return retry(
        stop_max_attempt_number=max_retries + 1,
        wait_exponential_multiplier=2,
        wait_exponential_max=max_sleep,
        wait_jitter_max=2,
        retry_on_exception=retry_on_exception
    )


class ThrottledBotoResource:
    def __init__(self, boto_resource, max_sleep=DEFAULT_WAIT_TIME, max_retries=INFINITE_RETRY):
        self.boto_resource = boto_resource
        self._decorator = boto_throttle(max_sleep=max_sleep, max_retries=max_retries)

    def __getattr__(self, item):
        attr = getattr(self.boto_resource, item)
        if callable(attr):
            return self._decorator(attr)
        else:
            return attr
