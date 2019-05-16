import functools
import json
import logging
import sys
import traceback
from contextlib import suppress
from time import sleep

import os
from botocore.exceptions import ClientError

from osdu_commons.utils.boto import create_boto_client
from osdu_commons.utils.throttle import ThrottledBotoResource

__all__ = [
    'StepFunctionsException',
    'StepFunctionsTaskTimeoutException',
    'StepFunctionsClient',
]

logger = logging.getLogger(__name__)

GET_ACTIVITY_TASK_SF_CLIENT_TIMEOUT = 65


class StepFunctionsException(Exception):
    pass


class StepFunctionsTaskTimeoutException(StepFunctionsException):
    pass


def error_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'TaskTimedOut':
                raise StepFunctionsTaskTimeoutException() from e
            else:
                raise StepFunctionsException() from e

    return wrapper


class StepFunctionsClient:
    def __init__(self, sf_client=None):
        sf_client = sf_client or create_boto_client('stepfunctions', region_name=os.environ['AWS_REGION'])
        self._client = ThrottledBotoResource(sf_client)

        # Boto docs advise get_activity_task timeout to be set to at least 65 seconds
        # so there is a separate client to call that function
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.get_activity_task
        sf_client_with_longer_timeout = create_boto_client('stepfunctions', region_name=os.environ['AWS_REGION'],
                                                           connect_timeout=GET_ACTIVITY_TASK_SF_CLIENT_TIMEOUT,
                                                           read_timeout=GET_ACTIVITY_TASK_SF_CLIENT_TIMEOUT)
        self._activity_task_client = ThrottledBotoResource(sf_client_with_longer_timeout)

    @error_handler
    def get_activity_task(self, activity_arn, worker_name=None):
        try:
            if worker_name is not None:
                activity_task = self._activity_task_client.get_activity_task(activityArn=activity_arn,
                                                                             workerName=worker_name)
            else:
                activity_task = self._activity_task_client.get_activity_task(activityArn=activity_arn)

            with suppress(KeyError, ValueError):
                activity_task['input'] = json.loads(activity_task['input'])
            return activity_task
        except StepFunctionsTaskTimeoutException:
            logger.exception(f'Get activity {activity_arn} timeout')
            raise

    def iterate_activity_tasks(self, activity_arn, worker_name=None, wait_sleep_time=2):
        while True:
            response = self.get_activity_task(activity_arn, worker_name)
            if 'taskToken' in response:
                logger.info(f'Received token {response["taskToken"][-10:]}')
                yield response
            else:
                sleep(wait_sleep_time)

    @error_handler
    def send_task_heartbeat(self, task_token):
        self._client.send_task_heartbeat(taskToken=task_token)

    def send_task_heartbeats_many(self, tasks_tokens):
        for token in tasks_tokens:
            self.send_task_heartbeat(task_token=token)

    @error_handler
    def send_task_success(self, task_token, output):
        logger.info(f'Sending successful response for task {task_token[-10:]}.')
        return self._client.send_task_success(taskToken=task_token, output=json.dumps(output))

    @error_handler
    def send_task_failure(self, task_token, cause=None, error=''):
        logger.error(f'Sending failed response for token {task_token[-10:]}')
        if cause:
            return self._client.send_task_failure(taskToken=task_token, cause=cause, error=error)

        exctype, value, tb = sys.exc_info()
        stacktrace = traceback.format_exception(exctype, value, tb)
        return self.send_task_failure_exception(task_token, exception=value, stacktrace=stacktrace)

    def send_task_failure_exception(self, task_token, exception, stacktrace):
        logger.error(f'Sending failed response for token: {task_token[-10:]}')
        cause = {
            'exception': repr(exception),
            'stacktrace': stacktrace
        }
        return self._client.send_task_failure(
            taskToken=task_token,
            cause=json.dumps(cause),
            error=type(exception).__name__,
        )

    @error_handler
    def start_execution(self, state_machine_arn: str, input, name=None) -> str:
        logger.info(f'Start execution of {state_machine_arn}')
        start_execution = functools.partial(
            self._client.start_execution,
            stateMachineArn=state_machine_arn,
            input=json.dumps(input),
        )
        if name is not None:
            start_execution = functools.partial(start_execution, name=name)
        response = start_execution()

        execution_arn = response['executionArn']
        logger.debug(f'Execution Arn: {execution_arn}')
        return execution_arn

    @error_handler
    def stop_execution(self, execution_arn, error=None, cause=None):
        logger.info(f'Stop execution of {execution_arn}')
        self._client.stop_execution(executionArn=execution_arn, error=error, cause=cause)
