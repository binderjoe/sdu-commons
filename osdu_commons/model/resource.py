import json
from typing import Optional, List, TypeVar, Type

import arrow as arrow
import attr
from attr import converters
from attr.validators import instance_of, optional

from osdu_commons.model.aws import S3Location
from osdu_commons.model.enums import ResourceLifecycleStatus, ResourceCurationStatus
from osdu_commons.utils import convert
from osdu_commons.utils.srn import SRN
from osdu_commons.utils.validators import list_of

T = TypeVar('T', bound='Resource')


@attr.s(frozen=True)
class Resource:
    id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    type_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    home_region_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    hosting_region_ids: List[SRN] = attr.ib(validator=list_of(instance_of(SRN)), converter=convert.list_(convert.srn))
    object_creation_date_time: arrow.Arrow = attr.ib(validator=instance_of(arrow.Arrow), converter=arrow.get)
    version_creation_date_time: arrow.Arrow = attr.ib(validator=instance_of(arrow.Arrow), converter=arrow.get)
    curation_status: ResourceCurationStatus = attr.ib(
        validator=instance_of(ResourceCurationStatus),
        converter=convert.resource_curation_status,
    )
    lifecycle_status: ResourceLifecycleStatus = attr.ib(
        validator=instance_of(ResourceLifecycleStatus),
        converter=convert.resource_lifecycle_status,
    )
    data: dict = attr.ib(validator=instance_of(dict))
    s3_location: Optional[S3Location] = attr.ib(validator=optional(instance_of(S3Location)), default=None)
    security_classification: Optional[SRN] = attr.ib(
        validator=optional(instance_of(SRN)),
        converter=converters.optional(convert.srn),
        default=None
    )

    @classmethod
    def from_dict(cls: Type[T], dict_: dict) -> T:
        data = dict_['Data']
        if isinstance(data, str):
            data = json.loads(dict_['Data'])

        return cls(
            id=SRN.from_string(dict_['ResourceID']),
            type_id=SRN.from_string(dict_['ResourceTypeID']),
            home_region_id=SRN.from_string(dict_['ResourceHomeRegionID']),
            hosting_region_ids=[SRN.from_string(region) for region in dict_['ResourceHostingRegionIDs']],
            object_creation_date_time=arrow.get(dict_['ResourceObjectCreationDateTime']),
            version_creation_date_time=arrow.get(dict_['ResourceVersionCreationDateTime']),
            curation_status=SRN.from_string(dict_['ResourceCurationStatus']),
            lifecycle_status=SRN.from_string(dict_['ResourceLifecycleStatus']),
            security_classification=SRN.from_string(dict_['ResourceSecurityClassification']),
            data=data
        )

    def asdict(self, data_to_string=False):
        result = {
            'ResourceID': str(self.id),
            'ResourceTypeID': str(self.type_id),
            'ResourceHomeRegionID': str(self.home_region_id),
            'ResourceHostingRegionIDs': [str(item) for item in self.hosting_region_ids],
            'ResourceObjectCreationDateTime': str(self.object_creation_date_time),
            'ResourceVersionCreationDateTime': str(self.version_creation_date_time),
            'ResourceCurationStatus': str(self.curation_status.value),
            'ResourceLifecycleStatus': str(self.lifecycle_status.value),
            'ResourceSecurityClassification': str(self.security_classification),
            'Data': self.data,
        }
        if data_to_string:
            result['Data'] = json.dumps(result['Data'])
        return result


@attr.s(frozen=True)
class ResourceInit:
    type: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    new_version: bool = attr.ib(validator=instance_of(bool))
    id: Optional[SRN] = attr.ib(validator=optional(instance_of(SRN)), default=None)
    key: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)


@attr.s(frozen=True)
class ResourceUpdate:
    id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    data: Optional[dict] = attr.ib(validator=optional(instance_of(dict)), default=None)
    curation_status: Optional[ResourceCurationStatus] = attr.ib(
        validator=optional(instance_of(ResourceCurationStatus)),
        converter=converters.optional(convert.resource_curation_status),
        default=None
    )
    lifecycle_status: Optional[ResourceLifecycleStatus] = attr.ib(
        validator=optional(instance_of(ResourceLifecycleStatus)),
        converter=converters.optional(convert.resource_lifecycle_status),
        default=None
    )
