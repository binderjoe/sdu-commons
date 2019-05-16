import re
from typing import Optional

import attr
from attr.validators import instance_of, optional

SRN_SECTION = '([^:]+)'
SRN_PATTERN = f'srn:{SRN_SECTION}:{SRN_SECTION}:(\d*)'


@attr.s(frozen=True)
class SRN:
    type: str = attr.ib(validator=instance_of(str))
    detail: str = attr.ib(validator=instance_of(str))
    version: Optional[int] = attr.ib(validator=optional(instance_of(int)), default=None)

    @property
    def without_version(self) -> 'SRN':
        return SRN(self.type, self.detail)

    def with_version(self, version: int) -> 'SRN':
        return SRN(self.type, self.detail, version)

    @staticmethod
    def from_string(srn):
        matched = re.fullmatch(SRN_PATTERN, srn)
        if matched is None:
            raise SRNFormatException(f'Incorrect format of srn: {srn} of type {type(srn)}')

        type_, detail, version = matched.groups()
        version = int(version) if version is not '' else None

        return SRN(type_, detail, version)

    def __str__(self):
        return f'srn:{self.type}:{self.detail}:{self.version or ""}'

    def __repr__(self):
        return str(self)


class SRNFormatException(ValueError):
    pass
