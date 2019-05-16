from typing import Optional

import attr
from attr.validators import optional, instance_of

from osdu_commons.utils import convert
from osdu_commons.utils.srn import SRN


@attr.s()
class FileGroupTypeProperties:
    original_file_path: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)
    staging_file_path: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)
    temp_workflow_location: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)
    file_source: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)
    description: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)
    schema: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)
    file_size: Optional[int] = attr.ib(validator=optional(instance_of(int)), default=None)
    require_key: Optional[bool] = attr.ib(validator=optional(instance_of(bool)), default=None)
    checksum: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)

    def asdict(self):
        result = {
            'Description': self.description,
            'Schema': self.schema,
            'RequireKey': self.require_key,
            'OriginalFilePath': self.original_file_path,
            'StagingFilePath': self.staging_file_path,
            'TempWorkflowLocation': self.temp_workflow_location,
            'FileSource': self.file_source,
            'FileSize': self.file_size,
            'Checksum': self.checksum
        }
        result_without_nones = {k: v for k, v in result.items() if v is not None}
        return result_without_nones


@attr.s(frozen=True)
class FileData:
    group_type_properties: FileGroupTypeProperties = attr.ib(
        validator=instance_of(FileGroupTypeProperties),
        converter=convert.class_from_camel_dict(FileGroupTypeProperties)
    )
    individual_type_properties: dict = attr.ib(validator=instance_of(dict), converter=convert.copy)
    extension_properties: dict = attr.ib(validator=instance_of(dict), converter=convert.copy)

    def asdict(self):
        return {
            'GroupTypeProperties': self.group_type_properties.asdict(),
            'IndividualTypeProperties': self.individual_type_properties,
            'ExtensionProperties': self.extension_properties,
        }


@attr.s(frozen=True)
class ManifestFile:
    associative_id: str = attr.ib(validator=instance_of(str))
    resource_type_id: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    data: FileData = attr.ib(
        validator=instance_of(FileData),
        converter=convert.class_from_camel_dict(FileData)
    )
    resource_security_classification: SRN = attr.ib(validator=instance_of(SRN),
                                                    converter=convert.resource_security_classification)

    def asdict(self):
        return {
            'AssociativeID': self.associative_id,
            'ResourceTypeID': str(self.resource_type_id),
            'Data': self.data.asdict(),
            'ResourceSecurityClassification': str(self.resource_security_classification),
        }
