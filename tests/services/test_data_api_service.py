import json

import arrow
import pytest
import responses
from tests.test_root import TEST_DATA_API_BASE_URL

from osdu_commons.model.enums import ResourceCurationStatus, ResourceLifecycleStatus
from osdu_commons.model.resource import Resource
from osdu_commons.model.smds_manifest import SMDSManifest
from osdu_commons.utils.srn import SRN


@responses.activate
def test_get_all_resources_success_without_unprocessed(data_api_service):
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
            'UnprocessedSRNs': []
        },
        status=200
    )

    resources = data_api_service.get_all_resources([
        SRN('master-data/Well', '123456789123'),
        SRN('master-data/Wellbore', '12345678912301')]
    )

    assert resources == [
        Resource(
            id=SRN(type='master-data/Well', detail='123456789123'),
            type_id=SRN(type='type', detail='master-data/Well'),
            home_region_id=SRN(type='reference-data/OSDURegion', detail='us-east-1'),
            hosting_region_ids=[],
            object_creation_date_time=arrow.get('2018-11-29 10:57:45'),
            version_creation_date_time=arrow.get('2018-11-29 10:57:46'),
            curation_status=ResourceCurationStatus.CREATED,
            lifecycle_status=ResourceLifecycleStatus.LOADING,
            data={},
            s3_location=None
        ),
        Resource(
            id=SRN(type='master-data/Wellbore', detail='12345678912301'),
            type_id=SRN(type='type', detail='master-data/Wellbore'),
            home_region_id=SRN(type='reference-data/OSDURegion', detail='us-east-1'),
            hosting_region_ids=[],
            object_creation_date_time=arrow.get('2018-11-29 10:57:47'),
            version_creation_date_time=arrow.get('2018-11-29 10:57:48'),
            curation_status=ResourceCurationStatus.CREATED,
            lifecycle_status=ResourceLifecycleStatus.LOADING,
            data={},
            s3_location=None
        )
    ]


@responses.activate
def test_get_all_resources_success_with_unprocessed(data_api_service):
    def request_callback(request):
        payload = json.loads(request.body)
        resource_id = payload['ResourceIDs'][-1]
        unprocessed_srns = payload['ResourceIDs'][:-1]

        resources = {
            'srn:master-data/Well:123456789123:': {
                'ResourceIDs': [
                    'srn:master-data/Well:123456789123:'
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
                    }
                ],
                'S3Location': [None],
                'RegionID': 'srn:region:us-east-1:',
                'UnprocessedSRNs': unprocessed_srns
            },
            'srn:master-data/Wellbore:12345678912301:': {
                'ResourceIDs': [
                    'srn:master-data/Wellbore:12345678912301:'
                ],
                'ResourceData': [
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
                'S3Location': [None],
                'RegionID': 'srn:region:us-east-1:',
                'UnprocessedSRNs': unprocessed_srns
            },
            'srn:master-data/Wellbore:12345678912302:': {
                'ResourceIDs': [
                    'srn:master-data/Wellbore:12345678912302:'
                ],
                'ResourceData': [
                    {
                        'ResourceID': 'srn:master-data/Wellbore:12345678912302:',
                        'ResourceTypeID': 'srn:type:master-data/Wellbore:',
                        'ResourceHomeRegionID': 'srn:reference-data/OSDURegion:us-east-1:',
                        'ResourceHostRegionIDs': [],
                        'ResourceObjectCreationDatetime': '2018-11-29 10:57:49',
                        'ResourceVersionCreationDatetime': '2018-11-29 10:57:50',
                        'ResourceCurationStatus': 'srn:reference-data/ResourceCurationStatus:CREATED:',
                        'ResourceLifecycleStatus': 'srn:reference-data/ResourceLifecycleStatus:LOADING:',
                        'Data': {}
                    }
                ],
                'S3Location': [None],
                'RegionID': 'srn:region:us-east-1:',
                'UnprocessedSRNs': unprocessed_srns
            },
        }

        body = resources[resource_id]

        headers = {}
        return (200, headers, json.dumps(body))

    responses.add_callback(
        responses.POST, f'{TEST_DATA_API_BASE_URL}/v1/getresources',
        callback=request_callback,
        content_type='application/json',
    )

    resources = data_api_service.get_all_resources([
        SRN('master-data/Well', '123456789123'),
        SRN('master-data/Wellbore', '12345678912301'),
        SRN('master-data/Wellbore', '12345678912302')
    ])

    assert resources == [
        Resource(
            id=SRN(type='master-data/Well', detail='123456789123'),
            type_id=SRN(type='type', detail='master-data/Well'),
            home_region_id=SRN(type='reference-data/OSDURegion', detail='us-east-1'),
            hosting_region_ids=[],
            object_creation_date_time=arrow.get('2018-11-29 10:57:45'),
            version_creation_date_time=arrow.get('2018-11-29 10:57:46'),
            curation_status=ResourceCurationStatus.CREATED,
            lifecycle_status=ResourceLifecycleStatus.LOADING,
            data={},
            s3_location=None
        ),
        Resource(
            id=SRN(type='master-data/Wellbore', detail='12345678912301'),
            type_id=SRN(type='type', detail='master-data/Wellbore'),
            home_region_id=SRN(type='reference-data/OSDURegion', detail='us-east-1'),
            hosting_region_ids=[],
            object_creation_date_time=arrow.get('2018-11-29 10:57:47'),
            version_creation_date_time=arrow.get('2018-11-29 10:57:48'),
            curation_status=ResourceCurationStatus.CREATED,
            lifecycle_status=ResourceLifecycleStatus.LOADING,
            data={},
            s3_location=None
        ),
        Resource(
            id=SRN(type='master-data/Wellbore', detail='12345678912302'),
            type_id=SRN(type='type', detail='master-data/Wellbore'),
            home_region_id=SRN(type='reference-data/OSDURegion', detail='us-east-1'),
            hosting_region_ids=[],
            object_creation_date_time=arrow.get('2018-11-29 10:57:49'),
            version_creation_date_time=arrow.get('2018-11-29 10:57:50'),
            curation_status=ResourceCurationStatus.CREATED,
            lifecycle_status=ResourceLifecycleStatus.LOADING,
            data={},
            s3_location=None
        )
    ]


@responses.activate
def test_exception_is_raised_instead_of_infinite_loop(data_api_service):
    responses.add(
        responses.POST,
        f'{TEST_DATA_API_BASE_URL}/v1/getresources',
        json={
            'ResourceIDs': [],
            'ResourceData': [],
            'S3Location': [],
            'RegionID': 'srn:region:us-east-1:',
            'UnprocessedSRNs': ['srn:master/Unprocessed:detail-1:']
        },
        status=200
    )

    with pytest.raises(RuntimeError):
        data_api_service.get_all_resources([
            SRN('srn:master/Unprocessed', 'detail-1')
        ])


@responses.activate
def test_iter_resources_tree(data_api_service):

    def request_callback(request):
        payload = json.loads(request.body)
        if 'srn:work-product/Document:123456789123:' in payload['ResourceIDs']:
            return (
                200,
                {},
                json.dumps({
                    'ResourceIDs': [
                        'srn:work-product/Document:123456789123:',
                    ],
                    'ResourceData': [
                        {
                            'ResourceID': 'srn:work-product/Document:123456789123:',
                            'ResourceTypeID': 'srn:type:work-product/Document:',
                            'ResourceHomeRegionID': 'srn:reference-data/OSDURegion:us-east-1:',
                            'ResourceHostRegionIDs': [],
                            'ResourceObjectCreationDatetime': '2018-11-29 10:57:45',
                            'ResourceVersionCreationDatetime': '2018-11-29 10:57:46',
                            'ResourceCurationStatus': 'srn:reference-data/ResourceCurationStatus:CREATED:',
                            'ResourceLifecycleStatus': 'srn:reference-data/ResourceLifecycleStatus:LOADING:',
                            'Data': {
                                'GroupTypeProperties': {
                                    'Components': [
                                        'srn:work-product-component/Document:123456789123:'
                                    ]
                                }
                            }
                        }
                    ],
                    'S3Location': [None],
                    'RegionID': 'srn:region:us-east-1:',
                    'UnprocessedSRNs': []
                })
            )
        elif 'srn:work-product-component/Document:123456789123:' in payload['ResourceIDs']:
            return (
                200,
                {},
                json.dumps({
                    'ResourceIDs': [
                        'srn:work-product-component/Document:123456789123:',
                    ],
                    'ResourceData': [
                        {
                            'ResourceID': 'srn:work-product-component/Document:123456789123:',
                            'ResourceTypeID': 'srn:type:work-product-component/Document:',
                            'ResourceHomeRegionID': 'srn:reference-data/OSDURegion:us-east-1:',
                            'ResourceHostRegionIDs': [],
                            'ResourceObjectCreationDatetime': '2018-11-29 10:57:45',
                            'ResourceVersionCreationDatetime': '2018-11-29 10:57:46',
                            'ResourceCurationStatus': 'srn:reference-data/ResourceCurationStatus:CREATED:',
                            'ResourceLifecycleStatus': 'srn:reference-data/ResourceLifecycleStatus:LOADING:',
                            'Data': {
                                'GroupTypeProperties': {
                                    'Files': [],
                                    'Artefacts': []
                                },
                                'IndividualTypeProperties': {
                                    'Name': 'BOF_6681',
                                    'Description': 'Document'
                                },
                                'ExtensionProperties': {}
                            }
                        }
                    ],
                    'S3Location': [None],
                    'RegionID': 'srn:region:us-east-1:',
                    'UnprocessedSRNs': []
                })
            )

    responses.add_callback(
        responses.POST,
        f'{TEST_DATA_API_BASE_URL}/v1/getresources',
        callback=request_callback,
        content_type='application/json'
    )

    resources_iterator = data_api_service.iter_resources_tree(
        SRN('work-product/Document', '123456789123'),
        with_artefacts=True
    )
    resources = list(resources_iterator)
    assert len(resources) == 2


@responses.activate
def test_create_smds_from_manifest(data_api_service):
    responses.add(
        responses.POST,
        f'{TEST_DATA_API_BASE_URL}/v1/createresources',
        json={
            'ResourceIDs': [
                'srn:master-data/Wellbore:12345678912301:'
            ],
            'ResourceData': [
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
            'S3Location': [None],
            'RegionID': 'srn:region:us-east-1:',
            'UnprocessedSRNs': []
        },
        status=201
    )

    responses.add(
        responses.POST,
        f'{TEST_DATA_API_BASE_URL}/v1/updateresources',
        json={
            'ResourceIDs': [
                'srn:master-data/Wellbore:12345678912301:'
            ],
            'ResourceData': [
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

    manifest = SMDSManifest(
        resource_id=SRN.from_string('srn:master-data/Wellbore:12345678912301:'),
        resource_type_id=SRN.from_string('srn:type:master-data/Wellbore:'),
        resource_security_classification=SRN.from_string('srn:reference-data/ResourceSecurityClassification:RESTRICTED:'),
        data={}
    )
    resource_id = data_api_service.create_smds_from_manifest(manifest)

    assert resource_id == SRN.from_string('srn:master-data/Wellbore:12345678912301:')
