from typing import List, Optional

import attr
from attr.validators import instance_of, optional

from osdu_commons.utils import convert
from osdu_commons.utils.srn import SRN


@attr.s(frozen=True)
class SMDSManifestData:
    group_type_properties: dict = attr.ib(validator=instance_of(dict), converter=convert.copy)
    individual_type_properties: dict = attr.ib(validator=instance_of(dict), converter=convert.copy)
    extension_properties: dict = attr.ib(validator=instance_of(dict), converter=convert.copy)


@attr.s(frozen=True)
class SMDSManifest:
    resource_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    resource_type_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    resource_security_classification = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    data: dict = attr.ib(validator=instance_of(dict), converter=convert.copy)
    description: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)
    original_resource_id = attr.ib(default=None)  # TODO Remove this along with hacks during loading types in DataAPI


def manifest_from_camel_dict(camel_dict: dict) -> SMDSManifest:
    return convert.class_from_camel_dict(SMDSManifest)(camel_dict)
