import os
import signal
import time
from contextlib import suppress

import pytest
from mock import ANY, call, create_autospec, patch
from osdu_commons.activity_worker.activity import Activity, ActivityWorker
from osdu_commons.activity_worker.step_functions_client import (StepFunctionsClient,
                                                                StepFunctionsException,
                                                                StepFunctionsTaskTimeoutException)


@pytest.fixture()
def step_functions_client():
    step_functions_client_mock = create_autospec(StepFunctionsClient)
    step_functions_client_mock.iterate_activity_tasks.return_value = [
        {'taskToken': 'token 1', 'input': 'input-1'},
        {'taskToken': 'token 2', 'input': 'input-2'}
    ]
    return step_functions_client_mock


@pytest.fixture()
def activity():
    activity_mock = create_autospec(Activity)
    activity_mock.activity_handler.side_effect = ['output-1', 'output-2']
    return activity_mock


@pytest.fixture()
def worker(step_functions_client, activity):
    return ActivityWorker(activity, 'test_activity_worker', 'activity_arn_123', step_functions_client)


def test_tasks_are_executed(worker, activity, step_functions_client):
    activity.activity_handler.assert_not_called()

    worker.run()

    activity.activity_handler.assert_has_calls([call('input-1'), call('input-2')])
    step_functions_client.send_task_success.assert_has_calls([call('token 1', 'output-1'), call('token 2', 'output-2')])


def test_token_is_set_to_none_when_task_is_done(worker):
    worker.run()
    assert worker.token is None


def test_sigterm_is_handled_correctly(worker, activity, step_functions_client):
    activity.activity_handler.side_effect = lambda _: os.kill(os.getpid(), signal.SIGINT)

    with suppress(SystemExit):
        worker.run()

    step_functions_client.send_task_failure.assert_called_once_with(
        'token 1', cause='Signal 2', error='Process terminated'
    )
    step_functions_client.send_task_success.assert_not_called()


def test_heartbeats_are_sent(worker, step_functions_client, activity):
    activity.activity_handler.side_effect = lambda _: time.sleep(0.01)
    with patch('osdu_commons.activity_worker.activity.sleep', return_value=None):
        worker.run()
        step_functions_client.send_task_heartbeat.assert_has_calls([call('token 1'), call('token 2')])


def test_send_failure_when_error_during_heart_beats_sending(worker, activity, step_functions_client):
    activity.activity_handler.side_effect = lambda _: time.sleep(0.01)
    step_functions_client.send_task_heartbeat.side_effect = StepFunctionsException('Some exception')

    with patch('osdu_commons.activity_worker.activity.sleep', return_value=None):
        with suppress(SystemExit):
            worker.run()

    step_functions_client.send_task_heartbeat.assert_called_once_with('token 1')
    step_functions_client.send_task_failure_exception.assert_called_once_with('token 1', ANY, ANY)
    step_functions_client.send_task_failure.assert_not_called()


def test_activity_worker_is_terminated_when_sf_timed_out(worker, activity, step_functions_client):
    activity.activity_handler.side_effect = lambda _: time.sleep(0.01)
    step_functions_client.send_task_heartbeat.side_effect = StepFunctionsTaskTimeoutException()

    with patch('osdu_commons.activity_worker.activity.sleep', return_value=None):
        with suppress(SystemExit):
            worker.run()

    step_functions_client.send_task_heartbeat.assert_called_once_with('token 1')
    step_functions_client.send_task_failure_exception.assert_not_called()
    step_functions_client.send_task_failure.assert_not_called()


def test_task_failure_is_send_when_exception_occurs(worker, activity, step_functions_client):
    exception_raised_while_processing = Exception('Exception occurred while processing')
    activity.activity_handler.side_effect = exception_raised_while_processing

    with suppress(Exception):
        worker.run()

    step_functions_client.send_task_success.assert_not_called()
    step_functions_client.send_task_failure_exception.assert_called_with(
        'token 1',
        exception=exception_raised_while_processing,
        stacktrace=ANY
    )
