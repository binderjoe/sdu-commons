import json
import logging
import os
import time
from functools import wraps

logger = logging.getLogger(__name__)


def log_format(context):
    return f'[%(levelname)s] [%(asctime)s.%(msecs)03dZ] [{context}] [%(name)s] %(message)s'


def configure_logging(
        context, logger=None, handler=None, formatter=None,
        level=os.environ.get('LOG_LEVEL', logging.INFO), propagate=False
):
    if handler is None:
        handler = logging.StreamHandler()
    if logger is None:
        logger = logging.getLogger()
    if formatter is None:
        formatter = logging.Formatter(
            fmt=log_format(context),
            datefmt='%Y-%m-%dT%H:%M:%S'
        )

    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    logger.setLevel(level)
    logger.propagate = propagate

    logger.addHandler(handler)
    if logger.handlers:
        for handler in logger.handlers:
            handler.setFormatter(formatter)


def configure_lambda_logging(lambda_context):
    configure_logging(
        lambda_context.aws_request_id,
        handler=LambdaLoggingHandler(),
        formatter=LambdaLoggingFormatter(
            fmt=log_format(lambda_context.aws_request_id),
            datefmt='%Y-%m-%dT%H:%M:%S'
        ),
    )


def lambda_logging(func):
    @wraps(func)
    def wrapper(event, context):
        configure_lambda_logging(context)
        logger.info('Request:\n{}'.format(json.dumps(event, indent=4)))
        response = func(event, context)
        logger.info('Response:\n{}'.format(json.dumps(response, indent=4)))
        return response

    return wrapper


class LambdaLoggingHandler(logging.StreamHandler):
    def __init__(self):
        logging.StreamHandler.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        if self.filter(record):
            print(msg)


class LambdaLoggingFormatter(logging.Formatter):
    def formatException(self, exc_info):
        s = super(LambdaLoggingFormatter, self).formatException(exc_info)
        return s.replace('\n', '\r')

    def format(self, record):
        s = super(LambdaLoggingFormatter, self).format(record)
        return s.replace('\n', '\r')


def timeit(method):
    @wraps(method)
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        duration = int((te-ts) * 1000)
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = duration
        else:
            print(f'{method.__name__} took {duration} ms')
        return result

    return timed
