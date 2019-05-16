import json

import arrow
import responses

from osdu_commons.clients.data_api_client import GetResourcesResult, DataAPIClient
from osdu_commons.model.enums import ResourceCurationStatus, ResourceLifecycleStatus
from osdu_commons.model.resource import ResourceInit, Resource, ResourceUpdate
from osdu_commons.utils.srn import SRN

from tests.test_root import TEST_DATA_API_BASE_URL


@responses.activate
def test_create_resources_request(data_api_client):
    responses.add(
        responses.POST,
        f'{TEST_DATA_API_BASE_URL}/v1/createresources',
        json={
            'ResourceIDs': [],
            'ResourceData': []
        },
        status=201
    )

    data_api_client.create_resources(
        resource_inits=[
            ResourceInit(
                type=SRN('type', 'some-type', 1),
                new_version=False
            ),
            ResourceInit(
                type=SRN('type', 'some-type2', 1),
                new_version=True,
                id=SRN('resource', 'some-resource', 2),
                key='some-key'
            )
        ],
        region_id=SRN('region', 'te-test-1')
    )

    assert len(responses.calls) == 1
    assert json.loads(responses.calls[0].request.body) == {
        'ResourceType': ['srn:type:some-type:1', 'srn:type:some-type2:1'],
        'NewVersion': [False, True], 'RegionID': 'srn:region:te-test-1:',
        'Keys': [None, 'some-key'],
        'ResourceIDs': [None, 'srn:resource:some-resource:2']
    }


@responses.activate
def test_create_resources_response(data_api_client):
    responses.add(
        responses.POST,
        f'{TEST_DATA_API_BASE_URL}/v1/createresources',
        json={
            'ResourceIDs': [
                'srn:master-data/Well:123456789123:',
                'srn:master-data/Wellbore:12345678912301:'
            ],
            'ResourceData': [
                {
                    'ResourceID': 'srn:master-data/Well:123456789123:',
                    'ResourceTypeID': 'srn:type:master-data/Well:',
                    'ResourceHomeRegionID': 'srn:reference-data/OSDURegion:us-east-1:',
                    'ResourceHostRegionIDs': [],
                    'ResourceObjectCreationDatetime': '2018-11-29 10:57:45',
                    'ResourceVersionCreationDatetime': '2018-11-29 10:57:46',
                    'ResourceCurationStatus': 'srn:reference-data/ResourceCurationStatus:CREATED:',
                    'ResourceLifecycleStatus': 'srn:reference-data/ResourceLifecycleStatus:LOADING:',
                    'Data': {}
                },
                {
                    'ResourceID': 'srn:master-data/Wellbore:12345678912301:',
                    'ResourceTypeID': 'srn:type:master-data/Wellbore:',
                    'ResourceHomeRegionID': 'srn:reference-data/OSDURegion:us-east-1:',
                    'ResourceHostRegionIDs': [],
                    'ResourceObjectCreationDatetime': '2018-11-29 10:57:47',
                    'ResourceVersionCreationDatetime': '2018-11-29 10:57:48',
                    'ResourceCurationStatus': 'srn:reference-data/ResourceCurationStatus:CREATED:',
                    'ResourceLifecycleStatus': 'srn:reference-data/ResourceLifecycleStatus:LOADING:',
                    'Data': {}
                }
            ]
        },
        status=201
    )

    result = data_api_client.create_resources(
        resource_inits=[],
        region_id=SRN('region', 'te-test-1')
    )

    assert len(responses.calls) == 1
    assert result == [
        Resource(
            id=SRN('master-data/Well', '123456789123'),
            type_id=SRN('type', 'master-data/Well'),
            home_region_id=SRN('reference-data/OSDURegion', 'us-east-1'),
            hosting_region_ids=[],
            object_creation_date_time=arrow.get('2018-11-29 10:57:45'),
            version_creation_date_time=arrow.get('2018-11-29 10:57:46'),
            curation_status=ResourceCurationStatus.CREATED,
            lifecycle_status=ResourceLifecycleStatus.LOADING,
            data={}
        ),
        Resource(
            id=SRN('master-data/Wellbore', '12345678912301'),
            type_id=SRN('type', 'master-data/Wellbore'),
            home_region_id=SRN('reference-data/OSDURegion', 'us-east-1'),
            hosting_region_ids=[],
            object_creation_date_time=arrow.get('2018-11-29 10:57:47'),
            version_creation_date_time=arrow.get('2018-11-29 10:57:48'),
            curation_status=ResourceCurationStatus.CREATED,
            lifecycle_status=ResourceLifecycleStatus.LOADING,
            data={}
        )
    ]


@responses.activate
def test_update_resources_request(data_api_client):
    responses.add(
        responses.POST,
        f'{TEST_DATA_API_BASE_URL}/v1/updateresources',
        json={
            'ResourceIDs': [],
            'ResourceData': [],
            'RegionID': ''
        },
        status=200
    )

    data_api_client.update_resources(
        resource_updates=[
            ResourceUpdate(
                id=SRN('master-data', 'detail-1', 1),
                curation_status=ResourceCurationStatus.CREATED,
                data={'a': 'b'},
            ),
            ResourceUpdate(
                id=SRN('master-example', 'detail-2', 2),
                lifecycle_status=ResourceLifecycleStatus.LOADING,
                data={'c': 'd'},
            ),
        ],
        region_id=SRN('region', 'te-test-1')
    )

    assert len(responses.calls) == 1
    assert json.loads(responses.calls[0].request.body.decode('utf-8')) == {
        'ResourceIDs': [
            'srn:master-data:detail-1:1',
            'srn:master-example:detail-2:2'
        ],
        'ResourceData': [
            '{"Data": {"a": "b"}, "ResourceCurationStatus": "srn:reference-data/ResourceCurationStatus:CREATED:"}',
            '{"Data": {"c": "d"}, "ResourceLifecycleStatus": "srn:reference-data/ResourceLifecycleStatus:LOADING:"}'
        ],
        'RegionID': 'srn:region:te-test-1:'
    }


@responses.activate
def test_update_resources_response(data_api_client):
    responses.add(
        responses.POST,
        f'{TEST_DATA_API_BASE_URL}/v1/updateresources',
        json={
            "ResourceIDs": [
                "srn:master-data/Wellbore:12345678912301:"
            ],
            "ResourceData": [
                {
                    'ResourceID': 'srn:master-data/Wellbore:12345678912301:',
                    'ResourceTypeID': 'srn:type:master-data/Wellbore:',
                    'ResourceHomeRegionID': 'srn:reference-data/OSDURegion:us-east-1:',
                    'ResourceHostRegionIDs': [],
                    'ResourceObjectCreationDatetime': '2018-11-29 10:57:47',
                    'ResourceVersionCreationDatetime': '2018-11-29 10:57:48',
                    'ResourceCurationStatus': 'srn:reference-data/ResourceCurationStatus:CREATED:',
                    'ResourceLifecycleStatus': 'srn:reference-data/ResourceLifecycleStatus:LOADING:',
                    'Data': {'Old': 'Data'}
                }
            ]
        },
        status=200
    )

    result = data_api_client.update_resources(
        resource_updates=[],
        region_id=SRN('region', 'te-test-1')
    )

    assert result == [
        Resource(
            id=SRN('master-data/Wellbore', '12345678912301'),
            type_id=SRN('type', 'master-data/Wellbore'),
            home_region_id=SRN('reference-data/OSDURegion', 'us-east-1'),
            hosting_region_ids=[],
            object_creation_date_time=arrow.get('2018-11-29 10:57:47'),
            version_creation_date_time=arrow.get('2018-11-29 10:57:48'),
            curation_status=ResourceCurationStatus.CREATED,
            lifecycle_status=ResourceLifecycleStatus.LOADING,
            data={'Old': 'Data'}
        )
    ]


@responses.activate
def test_get_resources_request(data_api_client: DataAPIClient):
    responses.add(
        responses.POST,
        f'{TEST_DATA_API_BASE_URL}/v1/getresources',
        json={
            'ResourceIDs': [],
            'ResourceData': [],
            'S3Location': [],
            'RegionID': '',
            'UnprocessedSRNs': []
        },
        status=200
    )

    data_api_client.get_resources(
        resource_ids=[
            SRN('master-data', 'detail-1', 1),
            SRN('master-example', 'detail-2', 2)
        ]
    )

    assert len(responses.calls) == 1
    assert json.loads(responses.calls[0].request.body.decode('utf-8')) == {
        'ResourceIDs': ['srn:master-data:detail-1:1', 'srn:master-example:detail-2:2']
    }


@responses.activate
def test_get_resources_response(data_api_client: DataAPIClient):
    responses.add(
        responses.POST,
        f'{TEST_DATA_API_BASE_URL}/v1/getresources',
        json={
            'ResourceIDs': [
                'srn:master-data/Well:123456789123:',
                'srn:master-data/Wellbore:12345678912301:'
            ],
            'ResourceData': [
                {
                    'ResourceID': 'srn:master-data/Well:123456789123:',
                    'ResourceTypeID': 'srn:type:master-data/Well:',
                    'ResourceHomeRegionID': 'srn:reference-data/OSDURegion:us-east-1:',
                    'ResourceHostRegionIDs': [],
                    'ResourceObjectCreationDatetime': '2018-11-29 10:57:45',
                    'ResourceVersionCreationDatetime': '2018-11-29 10:57:46',
                    'ResourceCurationStatus': 'srn:reference-data/ResourceCurationStatus:CREATED:',
                    'ResourceLifecycleStatus': 'srn:reference-data/ResourceLifecycleStatus:LOADING:',
                    'Data': {}
                },
                {
                    'ResourceID': 'srn:master-data/Wellbore:12345678912301:',
                    'ResourceTypeID': 'srn:type:master-data/Wellbore:',
                    'ResourceHomeRegionID': 'srn:reference-data/OSDURegion:us-east-1:',
                    'ResourceHostRegionIDs': [],
                    'ResourceObjectCreationDatetime': '2018-11-29 10:57:47',
                    'ResourceVersionCreationDatetime': '2018-11-29 10:57:48',
                    'ResourceCurationStatus': 'srn:reference-data/ResourceCurationStatus:CREATED:',
                    'ResourceLifecycleStatus': 'srn:reference-data/ResourceLifecycleStatus:LOADING:',
                    'Data': {}
                }
            ],
            'S3Location': [None, None],
            'RegionID': 'srn:region:us-east-1:',
            'UnprocessedSRNs': ['srn:master/Unprocessed:detail-1:1']
        },
        status=200
    )

    result = data_api_client.get_resources(
        resource_ids=[
            SRN('master-data', 'detail-1', 1),
            SRN('master-example', 'detail-2', 2)
        ]
    )
    assert result == GetResourcesResult(
        resources=[
            Resource(
                id=SRN('master-data/Well', '123456789123'),
                type_id=SRN('type', 'master-data/Well'),
                home_region_id=SRN('reference-data/OSDURegion', 'us-east-1'),
                hosting_region_ids=[],
                object_creation_date_time=arrow.get('2018-11-29 10:57:45'),
                version_creation_date_time=arrow.get('2018-11-29 10:57:46'),
                curation_status=ResourceCurationStatus.CREATED,
                lifecycle_status=ResourceLifecycleStatus.LOADING,
                data={}
            ),
            Resource(
                id=SRN('master-data/Wellbore', '12345678912301'),
                type_id=SRN('type', 'master-data/Wellbore'),
                home_region_id=SRN('reference-data/OSDURegion', 'us-east-1'),
                hosting_region_ids=[],
                object_creation_date_time=arrow.get('2018-11-29 10:57:47'),
                version_creation_date_time=arrow.get('2018-11-29 10:57:48'),
                curation_status=ResourceCurationStatus.CREATED,
                lifecycle_status=ResourceLifecycleStatus.LOADING,
                data={}
            )
        ],
        unprocessed_srns=[SRN('master/Unprocessed', 'detail-1', 1)]
    )
