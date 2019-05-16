import json
from typing import Dict

import attr
from attr.validators import instance_of

from osdu_commons.clients.rest_client import RestClient
from osdu_commons.clients.retry import osdu_retry


@attr.s(frozen=True)
class AuthHeaders:
    access_token: str = attr.ib(validator=instance_of(str))
    id_token: str = attr.ib(validator=instance_of(str))
    resource_names: str = attr.ib(validator=instance_of(str))

    def asdict(self):
        return {
            'Authorization': self.access_token,
            'X-Amazon-Cognito-Id': self.id_token,
            'X-Resource-Names': self.resource_names
        }

    @classmethod
    def from_event_headers(cls, event_headers):
        event_headers_lower = {k.lower(): v for k, v in event_headers.items()}
        return cls(
            id_token=event_headers_lower['x-amazon-cognito-id'],
            access_token=event_headers_lower['authorization'],
            resource_names=''
        )


@attr.s(frozen=True)
class AuthRequest:
    action: str = attr.ib(validator=instance_of(str))
    method_arn: str = attr.ib(validator=instance_of(str))
    service_name: str = attr.ib(validator=instance_of(str))

    def asdict(self):
        return attr.asdict(self)


@attr.s(frozen=True)
class AuthResponse:
    policy: Dict = attr.ib(validator=instance_of(Dict))

    @classmethod
    def from_json(cls, response_json):
        return cls(policy=json.loads(response_json['policy']))


class AuthClient(RestClient):
    def __init__(self, base_url, timeout_seconds=1):
        super().__init__(base_url, timeout_seconds)

    @osdu_retry(
        max_retries=3,
        wait_fixed=400,
        wait_random_max=100
    )
    def authorize(self, auth_request: AuthRequest, headers: AuthHeaders) -> AuthResponse:
        response = self.post(json=auth_request.asdict(), headers=headers.asdict())
        return AuthResponse.from_json(response.json())
