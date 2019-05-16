import re
from typing import TypeVar, Type

import attr
from attr.validators import instance_of

T = TypeVar('T', bound='S3Location')


class S3ParseURLError(Exception):
    pass


@attr.s(frozen=True)
class S3Location:
    bucket: str = attr.ib(validator=instance_of(str))
    key: str = attr.ib(validator=instance_of(str))

    @property
    def url(self) -> str:
        return f's3://{self.bucket}/{self.key}'

    @staticmethod
    def convert(item) -> 'S3Location':
        if isinstance(item, S3Location):
            return item
        elif isinstance(item, dict):
            return S3Location.from_dict(item)
        else:
            raise TypeError(f'Cannot convert {type(item)} to S3Location')

    @classmethod
    def from_dict(cls: Type[T], dict_: dict) -> T:
        return cls(
            bucket=dict_['Bucket'],
            key=dict_['Key']
        )

    def asdict(self) -> dict:
        return {
            "Bucket": self.bucket,
            "Key": self.key,
        }

    @classmethod
    def from_url(cls: Type[T], s3_url: str) -> T:
        match = re.match(r's3://(?P<bucket>.*?)/(?P<key>.*)', s3_url)
        if match is None:
            raise S3ParseURLError(f'Cannot parse s3 url {s3_url}')

        return cls(
            bucket=match.group('bucket'),
            key=match.group('key'),
        )
