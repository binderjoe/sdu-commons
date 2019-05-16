import json
import logging
from typing import Iterable, Optional

import attr
from attr.validators import instance_of, optional
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import WaiterError

from osdu_commons.model.aws import S3Location
from osdu_commons.utils.boto import create_boto_resource, create_boto_client

logger = logging.getLogger(__name__)

COPYING_MAX_CONCURRENCY = 50
THREE_DAYS_IN_SECONDS = 3 * 24 * 60 * 60


@attr.s(frozen=True)
class CopySpecification:
    source: S3Location = attr.ib(validator=instance_of(S3Location))
    target: S3Location = attr.ib(validator=instance_of(S3Location))


@attr.s(frozen=True)
class PresignedURLPostFields:
    key: str = attr.ib(validator=instance_of(str))
    aws_access_key_id: str = attr.ib(validator=instance_of(str))
    policy: str = attr.ib(validator=instance_of(str))
    signature: str = attr.ib(validator=instance_of(str))
    security_token: Optional[str] = attr.ib(validator=optional(instance_of(str)), default=None)

    @classmethod
    def from_dict(cls, dict_: dict) -> 'PresignedURLPostFields':
        return cls(
            key=dict_['key'],
            aws_access_key_id=dict_['AWSAccessKeyId'],
            policy=dict_['policy'],
            signature=dict_['signature'],
            security_token=dict_.get('x-amz-security-token'),
        )

    def as_dict(self) -> dict:
        result = {
            'key': self.key,
            'AWSAccessKeyId': self.aws_access_key_id,
            'policy': self.policy,
            'signature': self.signature,
        }
        if self.security_token is not None:
            result['x-amz-security-token'] = self.security_token
        return result


@attr.s(frozen=True)
class PresignedURLPost:
    url: str = attr.ib(validator=instance_of(str))
    fields: PresignedURLPostFields = attr.ib(validator=instance_of(PresignedURLPostFields))

    @classmethod
    def from_dict(cls, dict_: dict) -> 'PresignedURLPost':
        return cls(
            url=dict_['url'],
            fields=PresignedURLPostFields.from_dict(dict_['fields'])
        )

    def as_dict(self) -> dict:
        return {
            'url': self.url,
            'fields': self.fields.as_dict()
        }


class S3Service:
    def __init__(self, s3_resource=None, s3_client=None):
        self._s3_resource = s3_resource or create_boto_resource('s3')
        self._s3_client = s3_client or create_boto_client('s3')

    def load_json(self, location: S3Location):
        logger.debug(f'Loading data from {location.bucket}, {location.key}')
        loaded_data = self._s3_resource.Object(location.bucket, location.key)
        return json.load(loaded_data.get()['Body'])

    def put_json(self, location: S3Location, data):
        logger.debug(f'Putting data into {location.bucket}, {location.key}')
        data_in_json = json.dumps(data)
        self._s3_resource.Bucket(location.bucket).put_object(Key=location.key, Body=data_in_json)

    def copy(self, copy_specifications: Iterable[CopySpecification],
             max_concurrency: int = COPYING_MAX_CONCURRENCY):
        transfer_config = TransferConfig(max_concurrency=max_concurrency)

        def handle_copy_for_entry(spec: CopySpecification):
            logger.info(f'Copying {spec}')
            self._s3_client.copy(
                Bucket=spec.target.bucket,
                Key=spec.target.key,
                CopySource={
                    'Bucket': spec.source.bucket,
                    'Key': spec.source.key,
                },
                Config=transfer_config
            )

        for copy_spec in copy_specifications:
            handle_copy_for_entry(copy_spec)

    def wait_for_object(self, objects_locations: Iterable[S3Location], delay_in_seconds: int = 60,
                        max_attempts: Optional[int] = None, max_wait_in_seconds: int = THREE_DAYS_IN_SECONDS):
        max_attempts_before_time_ends = max_wait_in_seconds // delay_in_seconds
        if max_attempts:
            actual_max_attempts = min(max_attempts, max_attempts_before_time_ends)
        else:
            actual_max_attempts = max_attempts_before_time_ends
        logger.info(f'Waiting for {objects_locations} with {actual_max_attempts} max attempts '
                    f'and {delay_in_seconds}s delay')
        try:
            for location in objects_locations:
                waiter = self._s3_client.get_waiter('object_exists')
                waiter.wait(
                    Bucket=location.bucket,
                    Key=location.key,
                    WaiterConfig={
                        'Delay': delay_in_seconds,
                        'MaxAttempts': actual_max_attempts,
                    }
                )
                logger.debug(f'Waiting for {location} ended')
        except WaiterError as e:
            raise S3ClientWaiterError() from e

    def generate_presigned_url(self, s3_location: S3Location) -> PresignedURLPost:
        result = self._s3_client.generate_presigned_post(
            Bucket=s3_location.bucket,
            Key=s3_location.key
        )
        logger.debug(f'Generated presigned url for {s3_location}')
        return PresignedURLPost.from_dict(result)


class S3ClientWaiterError(Exception):
    pass
