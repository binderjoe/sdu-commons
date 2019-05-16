from botocore.exceptions import ClientError

from osdu_commons.utils.throttle import throttle_exception, ThrottledBotoResource


class Counter:
    def __init__(self):
        self.counter = 0

    def count(self):
        self.counter += 1


def test_throttle_exception():
    class BogusException(Exception):
        pass

    max_retries = 1
    retry_on_bogus = throttle_exception([BogusException], max_sleep=0.1, max_retries=max_retries)

    @retry_on_bogus
    def bogus_function(counter):
        counter.count()
        if counter.counter <= max_retries:
            raise BogusException

    counter = Counter()
    bogus_function(counter)

    assert counter.counter == max_retries + 1


def test_throttled_boto_resource():
    max_retries = 1

    class BogusResource:
        def __init__(self, counter, max_retries):
            self.counter = counter
            self._max_retries = max_retries

        def bogus_function(self):
            self.counter.count()
            if self.counter.counter <= self._max_retries:
                raise ClientError(
                    error_response={
                        'Error': {
                            'Code': 'ThrottlingException'
                        }
                    },
                    operation_name='bogus'
                )

    counter = Counter()
    bogus_resource = ThrottledBotoResource(BogusResource(counter, max_retries))
    bogus_resource.bogus_function()

    assert counter.counter == max_retries + 1
