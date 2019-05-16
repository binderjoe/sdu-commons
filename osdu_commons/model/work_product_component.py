from typing import List, Optional

import attr
from attr.validators import instance_of, optional

from osdu_commons.utils import convert
from osdu_commons.utils.srn import SRN
from osdu_commons.utils.validators import list_of


@attr.s(frozen=True)
class WorkProductComponentArtefactProperties:
    role_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    resource_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    resource_type_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn,
                                    default='srn:type:artefact/Unknown:')

    def asdict(self):
        return {
            'RoleID': str(self.role_id),
            'ResourceTypeID': str(self.resource_type_id),
            'ResourceID': str(self.resource_id),
        }


@attr.s(frozen=True)
class WorkProductComponentArtefact:
    role_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    resource_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    resource_type_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn,
                                    default='srn:type:artefact/Unknown:')

    def asdict(self):
        return {
            'RoleID': str(self.role_id),
            'ResourceTypeID': str(self.resource_type_id),
            'ResourceID': str(self.resource_id),
        }


@attr.s(frozen=True)
class WorkProductComponentGroupTypeProperties:
    files: Optional[List[SRN]] = attr.ib(
        default=attr.Factory(list),
        validator=optional(list_of(instance_of(SRN))),
        converter=attr.converters.optional(convert.list_(convert.srn))
    )
    artefacts: Optional[List[WorkProductComponentArtefact]] = attr.ib(
        validator=optional(list_of(instance_of(WorkProductComponentArtefact))),
        converter=attr.converters.optional(convert.list_(convert.class_from_camel_dict(WorkProductComponentArtefact))),
        default=attr.Factory(list)
    )
    description: Optional[str] = attr.ib(default=None, validator=optional(instance_of(str)))
    schema: Optional[str] = attr.ib(default=None, validator=optional(instance_of(str)))
    require_key: Optional[bool] = attr.ib(default=None, validator=optional(instance_of(bool)))

    def asdict(self):
        return {
            'Description': self.description,
            'Schema': self.schema,
            'RequireKey': self.require_key,
            'Files': [
                str(file_) for file_ in self.files
            ],
            'Artefacts': [
                artefact.asdict() for artefact in self.artefacts
            ]
        }


@attr.s(frozen=True)
class WorkProductComponentData:
    group_type_properties: WorkProductComponentGroupTypeProperties = attr.ib(
        validator=instance_of(WorkProductComponentGroupTypeProperties),
        converter=convert.class_from_camel_dict(WorkProductComponentGroupTypeProperties))
    individual_type_properties: dict = attr.ib(validator=instance_of(dict), converter=convert.copy)
    extension_properties: dict = attr.ib(validator=instance_of(dict), converter=convert.copy)

    def asdict(self):
        return {
            'GroupTypeProperties': self.group_type_properties.asdict(),
            'IndividualTypeProperties': self.individual_type_properties,
            'ExtensionProperties': self.extension_properties,
        }


@attr.s(frozen=True)
class WorkProductComponentManifest:
    resource_type_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    resource_security_classification: SRN = attr.ib(validator=instance_of(SRN),
                                                    converter=convert.resource_security_classification)
    data: WorkProductComponentData = attr.ib(
        validator=instance_of(WorkProductComponentData),
        converter=convert.class_from_camel_dict(WorkProductComponentData))
    associative_id: str = attr.ib(validator=instance_of(str))
    file_associative_ids: List[str] = attr.ib(validator=list_of(instance_of(str)), converter=convert.list_())

    def asdict(self):
        return {
            'ResourceTypeID': str(self.resource_type_id),
            'ResourceSecurityClassification': str(self.resource_security_classification),
            'Data': self.data.asdict(),
            'AssociativeID': self.associative_id,
            'FileAssociativeIDs': self.file_associative_ids,
        }
