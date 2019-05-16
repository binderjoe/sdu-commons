import responses

from osdu_commons.clients.delivery_client import DeliveryClient
from osdu_commons.utils.srn import SRN
from tests.test_root import TEST_DELIVERY_SERVICE_BASE_URL


@responses.activate
def test_get_resources_one_resource_not_found(delivery_client: DeliveryClient):
    responses.add(
        responses.POST,
        f'{TEST_DELIVERY_SERVICE_BASE_URL}/GetResources',
        json={
            'Error': {
                'Code': 'DeliveryServiceGetResourcesNotFound',
                'Description': '',
                'NotFoundResourceIDs': [
                    'srn:file/abc:1:1',
                ]
            }
        },
        status=404
    )

    srns = [SRN('file/abc', '1', 1), SRN('file/xyz', '2', 1)]
    get_resources_response = delivery_client.get_resources(srns)

    assert len(responses.calls) == 1
    assert len(get_resources_response.not_found_resource_ids) == 1
    assert get_resources_response.not_found_resource_ids[0] == SRN('file/abc', '1', 1)


@responses.activate
def test_get_resources_unprocess_response(delivery_client: DeliveryClient):
    responses.add(
        responses.POST,
        f'{TEST_DELIVERY_SERVICE_BASE_URL}/GetResources',
        json={
            'Result': [
                {
                    'SRN': 'srn:file/xyz:2:1',
                    'Data': {
                        'GroupTypeProperties': {
                            'OriginalFilePath': None,
                            'StagingFilePath': None,
                            'FileSource': 'xyz_2',
                        },
                    },
                }
            ],
            'UnprocessedSRNs': ['srn:file/abc:1:1'],
            'TemporaryCredentials': {}
        },
        status=200
    )

    srns = [SRN('file/abc', '1', 1), SRN('file/xyz', '2', 1)]
    get_resources_response = delivery_client.get_resources(srns)

    assert len(responses.calls) == 1

    assert len(get_resources_response.result) == 1
    assert len(get_resources_response.unprocessed_srn) == 1
