import pytest
import arrow

from osdu_commons.model.enums import ResourceCurationStatus, ResourceLifecycleStatus
from osdu_commons.model.resource import Resource
from osdu_commons.utils.srn import SRN


@pytest.fixture()
def resource():
    return Resource(
        id=SRN('work-product/Horizon', 'ffgghh', 1),
        type_id=SRN('type', 'work-product/Horizon', 1),
        home_region_id=SRN('reference-data/OSDURegion', 'us-east-1', 1),
        hosting_region_ids=[SRN('reference-data/OSDURegion', 'us-east-1', 1)],
        object_creation_date_time=arrow.utcnow(),
        version_creation_date_time=arrow.utcnow(),
        curation_status=ResourceCurationStatus.CREATED,
        lifecycle_status=ResourceLifecycleStatus.LOADING,
        security_classification=SRN('reference-data/ResourceSecurityClassification', 'RESTRICTED'),
        data={
            'GroupTypeProperties': {
                'Components': []
            },
            'IndividualTypeProperties': {
                'Name': 'Test Horizon',
                'Description': 'For tests',
            }
        }
    )


def test_resource_to_dict(resource: Resource):
    as_dict = resource.asdict()
    as_dict.pop('ResourceObjectCreationDateTime')
    as_dict.pop('ResourceVersionCreationDateTime')
    assert as_dict == {
         'ResourceID': 'srn:work-product/Horizon:ffgghh:1',
         'ResourceCurationStatus': 'srn:reference-data/ResourceCurationStatus:CREATED:',
         'ResourceHomeRegionID': 'srn:reference-data/OSDURegion:us-east-1:1',
         'ResourceHostingRegionIDs': ['srn:reference-data/OSDURegion:us-east-1:1'],
         'ResourceLifecycleStatus': 'srn:reference-data/ResourceLifecycleStatus:LOADING:',
         'ResourceTypeID': 'srn:type:work-product/Horizon:1',
         'ResourceSecurityClassification': 'srn:reference-data/ResourceSecurityClassification:RESTRICTED:',
         'Data': {
             'GroupTypeProperties': {
                 'Components': []
             },
             'IndividualTypeProperties': {
                 'Description': 'For tests',
                 'Name': 'Test Horizon'
             }
         }
         }


def test_resource_from_dict(resource: Resource):
    as_dict = resource.asdict()
    resource_from_dict = Resource.from_dict(as_dict)
    assert resource == resource_from_dict
