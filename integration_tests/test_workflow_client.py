import pytest

from osdu_commons.clients.rest_client import HttpServerException, HttpNotFoundException


@pytest.mark.usefixtures('smds_data')
def test_list_workflows(workflow_client):
    workflows = workflow_client.list_workflows()
    assert workflows.batch


@pytest.mark.usefixtures('smds_data')
def test_list_workflows_with_filter(workflow_client):
    workflows = workflow_client.list_workflows({"some_filter": 123})
    assert workflows.batch


def test_describe_workflow_smds_data(workflow_client, smds_workflow_job_id, smds_srn):
    workflow_job_description = workflow_client.describe_workflow(smds_workflow_job_id)

    assert workflow_job_description.state.has_succeeded()
    assert workflow_job_description.workflow_job_id == smds_workflow_job_id
    assert str(workflow_job_description.work_product_id) == smds_srn


def test_describe_workflow_swps_data(workflow_client, swps_workflow):
    workflow_job_description = workflow_client.describe_workflow(swps_workflow)
    assert workflow_job_description.state.has_succeeded()
    assert workflow_job_description.workflow_job_id == swps_workflow


def test_describe_workflow_that_does_not_exist(workflow_client):
    with pytest.raises(HttpNotFoundException, match='Workflow not found for id test_workflow_id_1234'):
        workflow_client.describe_workflow('test_workflow_id_1234')


def test_describe_workflow_no_workflow_id(workflow_client):
    with pytest.raises(HttpServerException):
        workflow_client.describe_workflow('')
