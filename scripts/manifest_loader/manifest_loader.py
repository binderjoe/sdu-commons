import csv
import json
import logging
import os
import random
import time
from functools import partial
from multiprocessing.pool import ThreadPool
from typing import Dict, Iterable, Callable, Optional, List

import attr
import click
from attr.validators import instance_of, optional
from more_itertools import chunked

from osdu_commons.clients.cognito_client import CognitoClient, create_cognito_client_from_config
from osdu_commons.clients.workflow_client import WorkflowClient, StartWorkflowResponse, WorkflowStatus
from osdu_commons.utils.logging import timeit
from osdu_commons.utils.validators import list_of

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

MAX_RETRY_ATTEMPTS = 10
MANIFEST_BATCH_SIZE = 500


@attr.s()
class ManifestMeta:
    file_path: str = attr.ib(validator=instance_of(str))
    workflow_job_id: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)
    workflow_status: Optional[WorkflowStatus] = attr.ib(validator=optional(instance_of(WorkflowStatus)), default=None)
    error_info: Optional[dict] = attr.ib(validator=optional(instance_of(dict)), default=None)

    def asdict(self):
        return {
            'FilePath': self.file_path,
            'WorkflowJobId': self.workflow_job_id,
            'WorkflowStatus': self.workflow_status.value if self.workflow_status is not None else None,
            'error_info': self.error_info,
        }


@attr.s(frozen=True)
class StartWorkflowResults:
    succeeded_manifests: List[ManifestMeta] = attr.ib(validator=list_of(instance_of(ManifestMeta)))
    failed_manifests: List[ManifestMeta] = attr.ib(validator=list_of(instance_of(ManifestMeta)))


@attr.s(frozen=True)
class DescribeWorkflowResults:
    succeeded_manifests: List[ManifestMeta] = attr.ib(validator=list_of(instance_of(ManifestMeta)))
    failed_manifests: List[ManifestMeta] = attr.ib(validator=list_of(instance_of(ManifestMeta)))
    stuck_manifests: List[ManifestMeta] = attr.ib(validator=list_of(instance_of(ManifestMeta)))


@attr.s(frozen=True)
class WorkflowResults:
    start_workflow_results: StartWorkflowResults = attr.ib(validator=instance_of(StartWorkflowResults))
    describe_workflow_results: DescribeWorkflowResults = attr.ib(validator=instance_of(DescribeWorkflowResults))

    def save(self, output_path: str) -> None:
        def generate_csv_item(manifest_meta: ManifestMeta):
            error_info = manifest_meta.error_info if manifest_meta.error_info is not None else {}
            return {
                'WorkflowJobID': manifest_meta.workflow_job_id,
                'Status': manifest_meta.workflow_status,
                'Error': error_info.get('Error', ''),
                'ErrorCause': error_info.get('Cause', ''),
                'ManifestPath': manifest_meta.file_path,
            }

        file_exists = os.path.isfile(output_path)
        with open(output_path, 'a') as out_fd:
            fieldnames = ['WorkflowJobID', 'Status', 'ManifestPath', 'Error', 'ErrorCause']
            writer = csv.DictWriter(out_fd, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()
            writer.writerows(map(generate_csv_item, self.start_workflow_results.failed_manifests))
            writer.writerows(map(generate_csv_item, self.describe_workflow_results.failed_manifests))
            writer.writerows(map(generate_csv_item, self.describe_workflow_results.stuck_manifests))
            writer.writerows(map(generate_csv_item, self.describe_workflow_results.succeeded_manifests))


class ManifestLoader:
    def __init__(self, config: Dict, cognito_client: CognitoClient, workflow_client: WorkflowClient,
                 processor_num: int = 4):
        self._config = config
        self._processor_num = processor_num
        self._cognito_client = cognito_client
        self._workflow_client = workflow_client

    def load_from_directory(self, directory: str, loading_type: str) -> Iterable[WorkflowResults]:
        manifest_meta_iter = self._get_manifests_from_directory(directory)
        return self.load_manifests(manifest_meta_iter, loading_type)

    def load_from_csv(self, input_csv_path, loading_type):
        manifest_meta_iter = self._get_manifests_from_csv(input_csv_path)
        return self.load_manifests(manifest_meta_iter, loading_type)

    def load_manifests(self, manifest_meta_iter: Iterable[ManifestMeta], loading_type: str):
        load_method = {
            'SMDS': partial(self.start_loading_manifest, self._workflow_client.start_smds_workflow),
            'SWPS': partial(self.start_loading_manifest, self._workflow_client.start_swps_workflow),
        }
        return self._load_manifests_in_batches(load_method[loading_type], manifest_meta_iter)

    def start_loading_manifest(self, start_workflow_fun: Callable[..., StartWorkflowResponse],
                               manifest_meta: ManifestMeta) -> ManifestMeta:
        logger.info(f'Loading Manifest: {manifest_meta.file_path}')

        with open(manifest_meta.file_path) as fp:
            manifest = json.load(fp)

        manifest_request = self.prepare_manifest_request(manifest)

        for i in range(MAX_RETRY_ATTEMPTS):
            try:
                response = start_workflow_fun(manifest_request)
                manifest_meta.workflow_job_id = response.workflow_job_id
                logger.info(f'Workflow id {manifest_meta.workflow_job_id} for manifest {manifest_meta.file_path}')
                return manifest_meta
            except Exception as e:
                logger.exception(e)
                time_to_sleep = self.get_sleep_time(i)
                logger.info(f'Loading backoff for {manifest_meta.file_path} {time_to_sleep} sec.')
                time.sleep(time_to_sleep)
                self._refresh_cognito_tokens()

    def check_workflow(self, manifest_meta: ManifestMeta) -> ManifestMeta:
        logger.info(f'Check workflow: {manifest_meta.workflow_job_id}')
        for i in range(MAX_RETRY_ATTEMPTS):
            try:
                workflow_description = self._workflow_client.describe_workflow(manifest_meta.workflow_job_id)

                if workflow_description.state.has_succeeded():
                    manifest_meta.workflow_status = workflow_description.state
                    logger.info(f'Workflow: {manifest_meta.workflow_job_id} OK')
                    break
                elif workflow_description.state.has_failed():
                    manifest_meta.workflow_status = workflow_description.state
                    manifest_meta.error_info = workflow_description.error_info
                    logger.info(f'Workflow: {manifest_meta.workflow_job_id} FAILED')
                    break

            except Exception as e:
                logger.exception(e)
                self._refresh_cognito_tokens()
            else:
                logger.info(f'Waiting for {manifest_meta.workflow_job_id} {workflow_description.state} '
                            f'{i + 1}/{MAX_RETRY_ATTEMPTS}')
            if i < MAX_RETRY_ATTEMPTS - 1:
                logger.info(f'Check workflow backoff for {manifest_meta.workflow_job_id}')
                time.sleep(self.get_sleep_time(i))
        else:
            manifest_meta.workflow_status = WorkflowStatus.RUNNING

        return manifest_meta

    def _load_manifests_in_batches(self, load_manifest_fun: Callable, manifest_meta_iter: Iterable[ManifestMeta]) \
            -> Iterable[WorkflowResults]:
        for manifest_paths_batch in chunked(manifest_meta_iter, MANIFEST_BATCH_SIZE):
            start_workflow_results = self._start_loading_workflow(load_manifest_fun, manifest_paths_batch)
            describe_workflow_results = self._describe_workflow(start_workflow_results.succeeded_manifests)

            yield WorkflowResults(
                start_workflow_results=start_workflow_results,
                describe_workflow_results=describe_workflow_results
            )

    def _describe_workflow(self, manifest_meta_iter: Iterable[ManifestMeta]) -> DescribeWorkflowResults:
        succeeded_manifests, failed_manifests, stuck_manifests = [], [], []
        with ThreadPool(processes=self._processor_num) as pool:
            for result_manifest_meta in pool.imap_unordered(self.check_workflow, manifest_meta_iter):
                if result_manifest_meta.workflow_status.has_succeeded():
                    succeeded_manifests.append(result_manifest_meta)
                elif result_manifest_meta.workflow_status.has_failed():
                    failed_manifests.append(result_manifest_meta)
                else:
                    stuck_manifests.append(result_manifest_meta)

        return DescribeWorkflowResults(
            succeeded_manifests=succeeded_manifests,
            failed_manifests=failed_manifests,
            stuck_manifests=stuck_manifests
        )

    def _start_loading_workflow(self, load_manifest_fun: Callable, manifest_meta_iter: Iterable[ManifestMeta]) \
            -> StartWorkflowResults:
        succeeded_manifests, failed_manifests = [], []
        with ThreadPool(processes=self._processor_num) as pool:
            for manifest_meta in pool.imap_unordered(load_manifest_fun, manifest_meta_iter):
                if manifest_meta.workflow_job_id is not None:
                    succeeded_manifests.append(manifest_meta)
                else:
                    failed_manifests.append(manifest_meta)

        return StartWorkflowResults(
            succeeded_manifests=succeeded_manifests,
            failed_manifests=failed_manifests
        )

    @staticmethod
    def _get_manifests_from_directory(directory: str) -> Iterable[ManifestMeta]:
        for base, _, filename_list in os.walk(directory):
            json_paths = (os.path.join(base, filename) for filename in filename_list if filename.endswith('.json'))
            for path in json_paths:
                yield ManifestMeta(file_path=path)

    @staticmethod
    def _get_manifests_from_csv(input_csv_path: str) -> Iterable[ManifestMeta]:
        # TODO manage workflow status somehow (eg. list only Failed, not succeeded, etc.)
        with open(input_csv_path) as fd:
            reader = csv.DictReader(fd)
            for row in reader:
                if WorkflowStatus(row['Status']) != WorkflowStatus.SUCCEEDED:
                    yield ManifestMeta(file_path=row['ManifestPath'])

    @staticmethod
    def get_sleep_time(iteration_num):
        return 2 ** (iteration_num + 1) + random.randint(3, 10)

    @staticmethod
    def prepare_manifest_request(manifest):
        return manifest.get('Manifest', manifest)

    def _refresh_cognito_tokens(self):
        # TODO token refreshing should be handled inside workflow client
        cognito_headers = self._cognito_client.get_tokens(
            user_name=self._config['COGNITO_USER_NAME'],
            password=self._config['COGNITO_USER_PASSWORD']
        ).to_request_headers()
        self._workflow_client._cognito_headers = cognito_headers


def validate_input(input_csv, manifest_dir_path):
    print(input_csv, manifest_dir_path)
    if not (input_csv is None) ^ (manifest_dir_path is None):
        raise Exception(f'Exactly one of the input_csv and manifest_dir_path should be provided')


@click.command()
@click.option('--config_path', help='Path to config file', required=True, type=str)
@click.option('--manifest_dir_path', help='Path to directory with manifests to load', required=False, type=str)
@click.option('--input_csv_path', help='Path to csv', required=False, type=str)
@click.option('--proc_num', help='Number of processors', required=False, type=int, default=1)
@click.option('--loading_type', help='Loading type', required=True, type=click.Choice(['SWPS', 'SMDS']))
@click.option('--output_path', help='Output file', required=True, type=str)
@timeit
def main(config_path, input_csv_path, manifest_dir_path, proc_num, loading_type, output_path):
    validate_input(input_csv_path, manifest_dir_path)

    logging.basicConfig(level=logging.INFO)

    with open(config_path) as config_fd:
        config = json.load(config_fd)

    cognito_client = create_cognito_client_from_config(config)
    cognito_headers = cognito_client.get_tokens(
        user_name=config['COGNITO_USER_NAME'],
        password=config['COGNITO_USER_PASSWORD']
    ).to_request_headers()

    workflow_client = WorkflowClient(
        base_url=config['WORKFLOW_ENDPOINT'],
        cognito_headers=cognito_headers,
    )

    manifest_loader = ManifestLoader(
        config=config,
        processor_num=proc_num,
        cognito_client=cognito_client,
        workflow_client=workflow_client,
    )

    workflow_results = None
    if manifest_dir_path is not None:
        workflow_results = manifest_loader.load_from_directory(manifest_dir_path, loading_type)
    elif input_csv_path is not None:
        workflow_results = manifest_loader.load_from_csv(input_csv_path, loading_type)

    for workflow_result in workflow_results:
        workflow_result.save(output_path)


if __name__ == '__main__':
    main()
