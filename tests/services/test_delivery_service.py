from typing import Union, Iterable
from unittest.mock import Mock

import pytest
from more_itertools import chunked

from osdu_commons.clients.delivery_client import GetResourcesResponseSuccess, GetResourcesResponseNotFound, \
     GetResourcesResultItem
from osdu_commons.model.aws import S3Location
from osdu_commons.services.delivery_service import DeliveryService, DeliveredResource
from osdu_commons.utils.srn import SRN


def create_delivery_client_mock(
        delivery_client_responses: Iterable[Union[GetResourcesResponseSuccess, GetResourcesResponseNotFound]]):
    get_resource_mock = Mock(side_effect=list(delivery_client_responses))
    delivery_client_mock = Mock()
    delivery_client_mock.get_resources = get_resource_mock

    return delivery_client_mock


def create_resource_response_success(resource_meta: Iterable[dict], temporary_credentials, unprocessed_srn=None) \
        -> GetResourcesResponseSuccess:
    return GetResourcesResponseSuccess(
        result=[GetResourcesResultItem(**res) for res in resource_meta],
        unprocessed_srn=unprocessed_srn if unprocessed_srn is not None else [],
        temporary_credentials=temporary_credentials,
    )


@pytest.mark.parametrize('srns', [
    [],
    [SRN('a', 'b', 1)],
    [SRN('a', 'b', 1), SRN('a', 'b', 2)],
    [SRN('a', 'b', v) for v in range(DeliveryService.MAX_GET_RESOURCES_BATCH_SIZE)],
])
def test_get_resources_single_request(srns, delivery_service: DeliveryService):
    credentials = Mock(spec=dict)
    resources = [{'srn': srn, 'data': Mock(spec=dict), 's3_location': Mock(spec=S3Location)} for srn in srns]
    client_response = create_resource_response_success(resources, credentials)
    delivery_service._delivery_client = create_delivery_client_mock([client_response])

    get_resources_response = delivery_service.get_resources(srns)

    assert list(get_resources_response) == [
        DeliveredResource(srn=res['srn'], data=res['data'], s3_location=res['s3_location'],
                          temporary_credentials=credentials, exists=True) for res in resources]


@pytest.mark.parametrize('srns', [
    [SRN('a', 'b', v) for v in range(DeliveryService.MAX_GET_RESOURCES_BATCH_SIZE + 1)],
    [SRN('a', 'b', v) for v in range(DeliveryService.MAX_GET_RESOURCES_BATCH_SIZE * 3)],
])
def test_get_resources_multiple_requests(srns, delivery_service: DeliveryService):
    credentials = Mock(spec=dict)
    resources = [{'srn': srn, 'data': Mock(spec=dict), 's3_location': Mock(spec=S3Location)} for srn in srns]
    resources_batches = chunked(resources, DeliveryService.MAX_GET_RESOURCES_BATCH_SIZE)
    client_responses = [create_resource_response_success(res, credentials) for res in resources_batches]
    delivery_service._delivery_client = create_delivery_client_mock(client_responses)

    get_resources_response = delivery_service.get_resources(srns)

    assert list(get_resources_response) == [
        DeliveredResource(srn=res['srn'], data=res['data'], s3_location=res['s3_location'],
                          temporary_credentials=credentials, exists=True) for res in resources]
