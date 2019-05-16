from typing import List, Optional
from unittest.mock import Mock

from osdu_commons.clients.workflow_client import StartWorkflowResponse, WorkflowJobDescription, WorkflowStatus
from osdu_commons.services.workflow_service import WorkflowService


def create_workflow_client_mock(
        start_smds_responses: Optional[List[StartWorkflowResponse]] = None,
        start_swps_responses: Optional[List[StartWorkflowResponse]] = None,
        list_responses: Optional[List] = None,
        describe_responses: Optional[List[WorkflowJobDescription]] = None
):
    if start_smds_responses is None:
        start_smds_responses = []
    if start_swps_responses is None:
        start_swps_responses = []
    if list_responses is None:
        list_responses = []
    if describe_responses is None:
        describe_responses = []

    workflow_client_mock = Mock()

    start_smds_workflow_mock = Mock(side_effect=iter(start_smds_responses))
    workflow_client_mock.start_smds_workflow = start_smds_workflow_mock

    start_swps_workflow_mock = Mock(side_effect=iter(start_swps_responses))
    workflow_client_mock.start_swps_workflow = start_swps_workflow_mock

    describe_workflow_mock = Mock(side_effect=iter(describe_responses))
    workflow_client_mock.describe_workflow = describe_workflow_mock

    list_workflows_mock = Mock(side_effect=iter(list_responses))
    workflow_client_mock.list_workflows = list_workflows_mock

    return workflow_client_mock


def test_start_and_wait_smds_workflow(workflow_service: WorkflowService):
    expected_workflow_descriptions = [WorkflowJobDescription('123', WorkflowStatus.SUCCEEDED)]
    workflow_client = create_workflow_client_mock(
        start_smds_responses=[StartWorkflowResponse('123')],
        describe_responses=expected_workflow_descriptions
    )
    workflow_service.workflow_client = workflow_client

    workflow_descriptions = workflow_service.start_and_wait_smds_workflow(master_data=[{}])

    assert expected_workflow_descriptions == workflow_descriptions


def test_start_and_wait_swps_workflow(workflow_service: WorkflowService):
    expected_workflow_descriptions = [WorkflowJobDescription('123', WorkflowStatus.SUCCEEDED)]
    manifest_mock = Mock()
    presigned_url_mock = Mock()
    data_dir = 'some_data_dir'

    workflow_client = create_workflow_client_mock(
        start_swps_responses=[StartWorkflowResponse('123', presigned_url_mock)],
        describe_responses=expected_workflow_descriptions
    )
    workflow_service.workflow_client = workflow_client
    upload_manifest_files_mock = Mock()
    workflow_service._upload_manifest_files = upload_manifest_files_mock

    workflow_descriptions = workflow_service.start_and_wait_swps_workflow(manifests=[manifest_mock], data_dir=data_dir)

    assert expected_workflow_descriptions == workflow_descriptions
    upload_manifest_files_mock.assert_called_once_with(manifest_mock, presigned_url_mock, data_dir)
