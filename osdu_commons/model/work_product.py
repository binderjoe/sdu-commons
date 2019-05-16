from typing import Optional, List

import attr
from attr.validators import instance_of, optional

from osdu_commons.utils import convert
from osdu_commons.utils.srn import SRN
from osdu_commons.utils.validators import list_of


@attr.s(frozen=True)
class WorkProductGroupTypeProperties:
    description: Optional[str] = attr.ib(default=None, validator=optional(instance_of(str)))
    schema: Optional[str] = attr.ib(default=None, validator=optional(instance_of(str)))
    require_key: Optional[bool] = attr.ib(default=None, validator=optional(instance_of(bool)))
    components: Optional[List[SRN]] = attr.ib(default=attr.Factory(list), validator=optional(list_of(instance_of(SRN))),
                                              converter=attr.converters.optional(convert.list_(convert.srn)))

    def asdict(self):
        return {
            'Description': self.description,
            'Schema': self.schema,
            'RequireKey': self.require_key,
            'Components': [
                str(srn) for srn in self.components
            ]
        }


@attr.s(frozen=True)
class WorkProductData:
    group_type_properties: WorkProductGroupTypeProperties = attr.ib(
        validator=instance_of(WorkProductGroupTypeProperties),
        converter=convert.class_from_camel_dict(WorkProductGroupTypeProperties))
    individual_type_properties: dict = attr.ib(validator=instance_of(dict), converter=convert.copy)
    extension_properties: dict = attr.ib(validator=instance_of(dict), converter=convert.copy)

    def asdict(self):
        return {
            'GroupTypeProperties': self.group_type_properties.asdict(),
            'IndividualTypeProperties': self.individual_type_properties,
            'ExtensionProperties': self.extension_properties,
        }


@attr.s(frozen=True)
class WorkProductManifest:
    resource_type_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    resource_security_classification: SRN = attr.ib(validator=instance_of(SRN),
                                                    converter=convert.resource_security_classification)
    data: WorkProductData = attr.ib(
        validator=instance_of(WorkProductData),
        converter=convert.class_from_camel_dict(WorkProductData))
    components_associative_ids: List[str] = attr.ib(validator=list_of(instance_of(str)), converter=convert.list_())

    def asdict(self):
        return {
            'ResourceTypeID': str(self.resource_type_id),
            'ResourceSecurityClassification': str(self.resource_security_classification),
            'Data': self.data.asdict(),
            'ComponentsAssociativeIDs': self.components_associative_ids,
        }
