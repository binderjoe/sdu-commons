import json
import logging
import os

import boto3
import pytest

from localstack.services import infra
from osdu_commons.clients.collection_client import CollectionClient
from osdu_commons.clients.data_api_client import DataAPIClient
from osdu_commons.clients.delivery_client import DeliveryClient
from osdu_commons.clients.search_client import SearchClient
from osdu_commons.clients.workflow_client import WorkflowClient
from osdu_commons.services.data_api_service import DataAPIService
from osdu_commons.services.delivery_service import DeliveryService
from osdu_commons.services.search_service import SearchService
from osdu_commons.services.workflow_service import WorkflowService
from osdu_commons.utils.srn import SRN
from tests.test_root import (TEST_COLLECTION_BASE_URL, TEST_DATA_API_BASE_URL, TEST_DELIVERY_SERVICE_BASE_URL,
                             TEST_MANIFESTS, TEST_SEARCH_SERVICE_BASE_URL, TEST_WORKFLOW_BASE_URL)

logging.basicConfig(
    level=logging.INFO
)

LOCALSTACK_S3_CONFIG = {'service_name': 's3',
                        'aws_access_key_id': 'aaa',
                        'aws_secret_access_key': 'bbb',
                        'endpoint_url': 'http://localhost:4572'}


@pytest.fixture(scope='session')
def localstack():
    infra.start_infra(asynchronous=True, apis=['s3'])
    yield
    infra.stop_infra()


@pytest.fixture(scope='session')
def s3_session(localstack):
    return boto3.session.Session()


@pytest.fixture(scope='session')
def localstack_s3_resource(s3_session):
    return s3_session.resource(**LOCALSTACK_S3_CONFIG)


@pytest.fixture(scope='session')
def localstack_s3_client(s3_session):
    return s3_session.client(**LOCALSTACK_S3_CONFIG)


@pytest.fixture()
def example_manifest():
    example_manifest_path = os.path.join(TEST_MANIFESTS, 'manifest_1.json')
    with open(example_manifest_path) as fp:
        return json.load(fp)


@pytest.fixture()
def data_api_client():
    return DataAPIClient(base_url=TEST_DATA_API_BASE_URL)


@pytest.fixture()
def data_api_service(data_api_client):
    return DataAPIService(
        data_api_client=data_api_client,
        region_id=SRN('reference-data/OSDURegion', 'us-east-1')
    )


@pytest.fixture()
def delivery_client():
    return DeliveryClient(base_url=TEST_DELIVERY_SERVICE_BASE_URL)


@pytest.fixture()
def delivery_service(delivery_client):
    return DeliveryService(delivery_client)


@pytest.fixture()
def search_client():
    return SearchClient(base_url=TEST_SEARCH_SERVICE_BASE_URL)


@pytest.fixture()
def search_service(search_client):
    return SearchService(search_client)


@pytest.fixture()
def workflow_client():
    return WorkflowClient(base_url=TEST_WORKFLOW_BASE_URL)


@pytest.fixture()
def workflow_service(workflow_client):
    return WorkflowService(workflow_client)


@pytest.fixture()
def collection_client():
    return CollectionClient(base_url=TEST_COLLECTION_BASE_URL)
