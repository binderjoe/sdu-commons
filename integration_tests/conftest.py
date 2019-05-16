import boto3
import pytest
import os
import json

from osdu_commons.clients.delivery_client import DeliveryClient
from osdu_commons.clients.workflow_client import WorkflowClient
from osdu_commons.services.workflow_service import WorkflowService
from osdu_commons.utils.testing import (create_cognito_client, create_cognito_user, create_cognito_headers,
                                       generate_random_smds_srn)


@pytest.fixture(scope='session')
def cognito_boto_client():
    return boto3.client('cognito-idp')


@pytest.fixture(scope='session')
def cognito_client(cognito_boto_client):
    yield create_cognito_client(cognito_boto_client)


@pytest.fixture(scope='session')
def cognito_user(cognito_client):
    user = create_cognito_user(cognito_client)
    yield user
    cognito_client.delete_user(user.name)


@pytest.fixture(scope='session')
def cognito_headers(cognito_client, cognito_user):
    yield create_cognito_headers(cognito_client, cognito_user)


@pytest.fixture(scope='session')
def delivery_client(cognito_headers):
    assert 'DELIVERY_SERVICE_URL' in os.environ, 'Missing environment variable: DELIVERY_SERVICE_URL'
    return DeliveryClient(base_url=os.environ['DELIVERY_SERVICE_URL'], cognito_headers=cognito_headers)


@pytest.fixture(scope='session')
def workflow_client(cognito_headers):
    assert 'WORKFLOW_SERVICE_URL' in os.environ, 'Missing environment variable: WORKFLOW_SERVICE_URL'
    return WorkflowClient(base_url=os.environ['WORKFLOW_SERVICE_URL'], cognito_headers=cognito_headers)


@pytest.fixture(scope='session')
def workflow_service(workflow_client):
    return WorkflowService(workflow_client=workflow_client)


@pytest.fixture(scope='session')
def data_dir():
    return os.path.join(os.path.dirname(__file__), 'data')


@pytest.fixture(scope='session')
def smds_data(workflow_service, data_dir):
    srn = generate_random_smds_srn()
    manifest_file_path = os.path.join(data_dir, 'smds_manifest.json')
    with open(manifest_file_path) as file:
        manifest = json.load(file)

    manifest['ResourceID'] = srn
    workflow_job_descs = workflow_service.start_and_wait_smds_workflow([manifest])
    workflow_job_id = workflow_job_descs[0].workflow_job_id
    return srn, workflow_job_id


@pytest.fixture()
def smds_workflow_job_id(smds_data):
    return smds_data[1]


@pytest.fixture()
def smds_srn(smds_data):
    return smds_data[0]


@pytest.fixture(scope='session')
def swps_workflow(workflow_service, data_dir):
    manifest_file_path = os.path.join(data_dir, 'swps_manifest.json')
    with open(manifest_file_path) as file:
        manifest = json.load(file)

    workflow_job_descs = workflow_service.start_and_wait_swps_workflow([manifest], data_dir)
    workflow_job_id = workflow_job_descs[0].workflow_job_id
    return workflow_job_id
