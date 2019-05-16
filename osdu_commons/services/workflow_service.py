import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import requests

from osdu_commons.clients.workflow_client import WorkflowClient, WorkflowJobDescription

logger = logging.getLogger(__name__)


class WorkflowTimeoutException(Exception):
    pass


class WorkflowService:
    SLEEP_TIME_SEC = 5
    MAX_RETRIES = 48

    def __init__(self, workflow_client: WorkflowClient):
        self.workflow_client = workflow_client

    def start_and_wait_smds_workflow(self, master_data: List[dict]) -> List[WorkflowJobDescription]:
        logger.info(f'Starting smds workflow for {len(master_data)} master_data(s)')
        workflow_job_descriptions = []
        workflow_ids = []
        for manifest in master_data:
            workflow_ids.append(self.workflow_client.start_smds_workflow(manifest).workflow_job_id)
        for workflow_id in workflow_ids:
            workflow_job_descriptions.append(self._await_completion(workflow_id))
        return workflow_job_descriptions

    def start_and_wait_swps_workflow(self, manifests: List[dict], data_dir: str,
                                     thread_count: Optional[int] = None) -> List[WorkflowJobDescription]:
        logger.info(f'Starting swps workflow for {len(manifests)} manifest(s)')
        workflow_job_descriptions = []
        workflow_ids = []
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            for manifest in manifests:
                workflow_ids.append(executor.submit(self._start_swps_and_upload, manifest, data_dir))
        workflow_ids = [item.result() for item in workflow_ids]
        timed_out_jobs = []
        for workflow_id in workflow_ids:
            try:
                workflow_job_descriptions.append(self._await_completion(workflow_id))
            except WorkflowTimeoutException:
                timed_out_jobs.append(workflow_id)

        if len(timed_out_jobs) > 0:
            raise WorkflowTimeoutException(f'Timeout while waiting for {", ".join(timed_out_jobs)}')
        return workflow_job_descriptions

    def _start_swps_and_upload(self, manifest, data_dir):
        response = self.workflow_client.start_swps_workflow(manifest)
        if response.presigned_urls:
            self._upload_manifest_files(manifest, response.presigned_urls, data_dir)
        return response.workflow_job_id

    def describe_workflow(self, workflow_id: str) -> WorkflowJobDescription:
        return self.workflow_client.describe_workflow(workflow_id)

    def list_workflows(self):
        return self.workflow_client.list_workflows()

    def _await_completion(self, workflow_id: str) -> WorkflowJobDescription:
        for _ in range(self.MAX_RETRIES):
            workflow_job_description = self.describe_workflow(workflow_id)
            state = workflow_job_description.state
            if state.is_finished():
                return workflow_job_description
            time.sleep(self.SLEEP_TIME_SEC)
        raise WorkflowTimeoutException(f'Timeout while waiting for {workflow_id}')

    @staticmethod
    def _upload_manifest_files(manifest: dict, presigned_urls: dict, data_dir: str):
        logger.info(f'Uploading {len(presigned_urls)} manifest files')
        associative_id_to_file_source = {
            file['AssociativeID']: file['Data']['GroupTypeProperties']['FileSource']
            for file in manifest['Files']
        }
        for associative_id, file_definition in presigned_urls.items():
            url = file_definition['url']
            data = file_definition['fields']
            file_path = os.path.join(data_dir, associative_id_to_file_source[associative_id])
            with open(file_path, 'rb') as source_file:
                logger.debug(f'Starting uploading manifest file: {source_file}')
                response = requests.post(url, data=data, files={'file': source_file})
                response.raise_for_status()
                logger.debug(f'Uploaded manifest file: {source_file}')
        logger.info('All files uploaded')
