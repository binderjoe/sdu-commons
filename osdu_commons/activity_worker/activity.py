import logging
import os
import signal
import sys
import traceback
from abc import ABC, abstractmethod
from contextlib import suppress
from threading import Thread
from time import sleep

from osdu_commons.activity_worker.step_functions_client import (StepFunctionsClient,
                                                                StepFunctionsException,
                                                                StepFunctionsTaskTimeoutException)

__all__ = [
    'Activity',
    'ActivityWorker'
]

logger = logging.getLogger(__name__)

SIGNALS_THAT_MAKE_TASK_FAIL = [signal.SIGHUP, signal.SIGINT, signal.SIGQUIT, signal.SIGABRT, signal.SIGFPE,
                               signal.SIGILL, signal.SIGSEGV, signal.SIGPIPE, signal.SIGTERM, signal.SIGTERM]


class Activity(ABC):
    @staticmethod
    @abstractmethod
    def create_instance():
        pass

    @abstractmethod
    def activity_handler(self, event):
        pass


class ActivityWorker:
    WAIT_SLEEP_TIME = int(os.environ.get('WAIT_SLEEP_TIME', 2))
    HEARTBEAT_INTERVAL = int(os.environ.get('HEARTBEAT_INTERVAL', 60))

    def __init__(self, activity: Activity, worker_name: str, activity_arn: str, sf_client: StepFunctionsClient = None):
        self._activity = activity
        self._worker_name = worker_name
        self._activity_arn = activity_arn

        self._sf_client = sf_client or StepFunctionsClient()
        self._heartbeat_daemon = Thread(target=self._send_heartbeats, daemon=True)

        self.token = None
        for sig in SIGNALS_THAT_MAKE_TASK_FAIL:
            signal.signal(sig, self._sigterm_handler)

    def run(self):
        logger.info(f'Starting ActivityWorker of {self._activity.__class__.__name__} activity')
        self._heartbeat_daemon.start()
        tasks_iterator = self._sf_client.iterate_activity_tasks(
            activity_arn=self._activity_arn,
            worker_name=self._worker_name
        )
        for task in tasks_iterator:
            self.token, input_event = task['taskToken'], task['input']
            logger.debug(f'Pulled new task: {token_tip(self.token)}')
            try:
                output = self._activity.activity_handler(input_event)
                with suppress(StepFunctionsTaskTimeoutException):
                    self._sf_client.send_task_success(self.token, output)
                self.token = None
            except Exception as e:
                logger.exception(f'Exception while processing task {token_tip(self.token)}')
                self._sf_client.send_task_failure_exception(self.token, exception=e, stacktrace=traceback.format_exc())
                raise

    def _send_heartbeats(self):
        logger.debug('Started heart beats sending daemon')

        def _kill_parent_process():
            """ Kill the whole process from the daemon, set current token to None to do
            not send failure, then kill parent and exit itself """
            logger.debug('Killing activity worker process from heart beats daemon')
            self.token = None
            os.kill(os.getpid(), signal.SIGTERM)
            sys.exit()

        while True:
            if self.token:
                try:
                    self._sf_client.send_task_heartbeat(self.token)
                except StepFunctionsTaskTimeoutException:
                    logger.error(f'Timeout when sending heartbeat for {token_tip(self.token)}, exiting worker now')
                    _kill_parent_process()
                except StepFunctionsException as e:
                    logger.exception(f'Exception occurred when sending heartbeat for {token_tip(self.token)}')
                    self._sf_client.send_task_failure_exception(
                        self.token,
                        exception=e,
                        stacktrace=traceback.format_exc()
                    )
                    _kill_parent_process()
            sleep(self.HEARTBEAT_INTERVAL)

    # noinspection PyUnusedLocal
    def _sigterm_handler(self, signum, frame):
        logger.info(f'Received {signum} signal')
        if self.token:
            self._sf_client.send_task_failure(self.token, cause=f'Signal {signum}', error='Process terminated')
        sys.exit(1)


def token_tip(token: str) -> str:
    return token[-10:]
