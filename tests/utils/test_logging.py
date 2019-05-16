import io
from contextlib import redirect_stdout

from osdu_commons.utils.logging import lambda_logging, timeit


@lambda_logging
def _fake_lambda(event, context):
    return {
        'statusCode': 200,
        'body': 'hello world'
    }


class BogusLambdaContext:
    aws_request_id = '1234'


@timeit
def _function():
    pass


def test_lambda_logging(caplog):
    _fake_lambda({'test': True}, BogusLambdaContext())

    assert 'Request:' in caplog.records[0].getMessage()
    assert 'Response:' in caplog.records[1].getMessage()


def test_timeit(caplog):
    f = io.StringIO()
    with redirect_stdout(f):
        _function()
    s = f.getvalue()

    assert len(s) > 0
