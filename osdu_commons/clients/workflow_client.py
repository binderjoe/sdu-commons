import enum
import logging
from typing import List, Optional

import attr
from attr.validators import instance_of, optional

from osdu_commons.clients.cognito_aware_rest_client import CognitoAwareRestClient
from osdu_commons.clients.retry import osdu_retry
from osdu_commons.utils import convert
from osdu_commons.utils.srn import SRN

logger = logging.getLogger(__name__)


class WorkflowStatus(enum.Enum):
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    ABORTED = 'ABORTED'
    TIMED_OUT = 'TIMED_OUT'
    RUNNING = 'RUNNING'
    CREATED = 'CREATED'

    def has_succeeded(self):
        return self == self.SUCCEEDED

    def has_failed(self):
        return self in [self.FAILED, self.ABORTED, self.TIMED_OUT]

    def is_finished(self):
        return self not in [self.RUNNING, self.CREATED]


@attr.s(frozen=True)
class WorkflowJobDescription:
    workflow_job_id: str = attr.ib()
    state: WorkflowStatus = attr.ib(validator=optional(instance_of(WorkflowStatus)))
    error_info: Optional[dict] = attr.ib(default=None)
    work_product_id: Optional[SRN] = attr.ib(
        validator=optional(instance_of(SRN)), converter=attr.converters.optional(convert.srn), default=None)

    @classmethod
    def from_json(cls, response_json):
        return cls(
            workflow_job_id=response_json['WorkflowJobID'],
            state=WorkflowStatus(response_json['State']),
            work_product_id=response_json.get('WorkProductID'),
            error_info=response_json.get('ErrorInfo')
        )


@attr.s(frozen=True)
class Workflows:
    batch: List[WorkflowJobDescription] = attr.ib()
    next_token: Optional[str] = attr.ib()

    @classmethod
    def from_json(cls, response_json):
        return cls(
            batch=[WorkflowJobDescription.from_json(job) for job in response_json['Batch']],
            next_token=response_json['NextToken']
        )


@attr.s(frozen=True)
class StartWorkflowResponse:
    workflow_job_id: str = attr.ib()
    presigned_urls: Optional[dict] = attr.ib(default=None)

    @classmethod
    def from_json(cls, response_json):
        return cls(
            workflow_job_id=response_json['WorkflowJobID'],
            presigned_urls=response_json.get('PresignedUrls')
        )


class WorkflowClient(CognitoAwareRestClient):
    TIMEOUT = 30
    START_SWPS_WORKFLOW_ENDPOINT = 'StartSWPSLoadingWorkflow'
    START_SMDS_WORKFLOW_ENDPOINT = 'StartSMDSLoadingWorkflow'
    DESCRIBE_ENDPOINT = 'DescribeWorkflow'
    LIST_ENDPOINT = 'ListWorkflows'

    def start_smds_workflow(self, manifest_json: dict) -> StartWorkflowResponse:
        resource_type_id = manifest_json['ResourceTypeID']
        logger.info(f'Starting SMDS workflow for {resource_type_id}')
        response = self.post(
            path=self.START_SMDS_WORKFLOW_ENDPOINT,
            json={
                'Manifest': manifest_json,
                'ResourceTypeID': resource_type_id
            }
        )
        response.raise_for_status()

        return StartWorkflowResponse.from_json(response.json())

    def start_swps_workflow(self, manifest_json: dict) -> StartWorkflowResponse:
        resource_type_id = manifest_json['WorkProduct']['ResourceTypeID']
        logger.info(f'Starting SWPS workflow for {resource_type_id}')
        response = self.post(
            path=self.START_SWPS_WORKFLOW_ENDPOINT,
            json={
                'Manifest': manifest_json,
                'ResourceTypeID': resource_type_id
            }
        )
        response.raise_for_status()

        return StartWorkflowResponse.from_json(response.json())

    @osdu_retry()
    def describe_workflow(self, workflow_id: str) -> WorkflowJobDescription:
        logger.debug(f'Calling describe workflow with id: {workflow_id}')
        response = self.post(
            path=self.DESCRIBE_ENDPOINT,
            json={
                'WorkflowJobID': workflow_id
            }
        )
        return WorkflowJobDescription.from_json(response.json())

    @osdu_retry()
    def list_workflows(self, filters: dict = None, next_token: str = None, max_page_size: int = None) -> Workflows:
        logger.info(f'Calling list workflows with {filters or "no"} filters')
        response = self.post(
            path=self.LIST_ENDPOINT,
            json={
                'Filters': filters or {},  # filter cannot be None
                'NextToken': next_token,
                'MaxPageSize': max_page_size
            }
        )
        return Workflows.from_json(response.json())
