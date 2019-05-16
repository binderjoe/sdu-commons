from typing import Dict

import boto3
from botocore.client import Config


def create_boto_client(service_name: str,
                       connect_timeout: int = 3,
                       read_timeout: int = 5,
                       retries: int = 3,
                       region_name: str = None,
                       config: Dict = None):
    _config = _prepare_config(connect_timeout, read_timeout, retries, region_name, config)
    return boto3.Session().client(service_name, config=_config)


def create_boto_resource(service_name: str,
                         connect_timeout: int = 3,
                         read_timeout: int = 5,
                         retries: int = 3,
                         region_name: str = None,
                         config: Dict = None):
    _config = _prepare_config(connect_timeout, read_timeout, retries, region_name, config)
    return boto3.Session().resource(service_name, config=_config)


def _prepare_config(connect_timeout, read_timeout, retries, region_name, config):
    _config = {
        'region_name': region_name,
        'connect_timeout': connect_timeout,
        'read_timeout': read_timeout,
        'retries': {'max_attempts': retries}
    }
    if config:
        _config.update(config)
    return Config(**_config)
