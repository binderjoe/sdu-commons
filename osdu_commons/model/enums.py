from enum import Enum

from osdu_commons.utils.srn import SRN


class ResourceLifecycleStatus(Enum):
    LOADING = SRN('reference-data/ResourceLifecycleStatus', 'LOADING')
    RECEIVED = SRN('reference-data/ResourceLifecycleStatus', 'RECEIVED')
    ACCEPTED = SRN('reference-data/ResourceLifecycleStatus', 'ACCEPTED')
    RESCINDED = SRN('reference-data/ResourceLifecycleStatus', 'RESCINDED')
    DELETED = SRN('reference-data/ResourceLifecycleStatus', 'DELETED')


class ResourceCurationStatus(Enum):
    CREATED = SRN('reference-data/ResourceCurationStatus', 'CREATED')
    CURATING = SRN('reference-data/ResourceCurationStatus', 'CURATING')
    CURATED = SRN('reference-data/ResourceCurationStatus', 'CURATED')


class ResourceSecurityClassification(Enum):
    RESTRICTED = 'RESTRICTED'
    CLASSIFIED = 'CLASSIFIED'
    CONFIDENTIAL = 'CONFIDENTIAL'
    MOST_CONFIDENTIAL = 'MOST-CONFIDENTIAL'


SECURITY_CLASSIFICATION_TO_SRN = {
    ResourceSecurityClassification.RESTRICTED:
        SRN(type='reference-data/ResourceSecurityClassification', detail='RESTRICTED'),
    ResourceSecurityClassification.CLASSIFIED:
        SRN(type='reference-data/ResourceSecurityClassification', detail='CLASSIFIED'),
    ResourceSecurityClassification.CONFIDENTIAL:
        SRN(type='reference-data/ResourceSecurityClassification', detail='CONFIDENTIAL'),
    ResourceSecurityClassification.MOST_CONFIDENTIAL:
        SRN(type='reference-data/ResourceSecurityClassification', detail='MOST-CONFIDENTIAL'),
}
