import base64
import hashlib
import hmac
import logging

import attr
from attr.validators import instance_of
from cachetools import TTLCache, cached
from retrying import retry

from osdu_commons.utils.boto import create_boto_client

logger = logging.getLogger(__name__)


def get_cognito_headers(config):
    cognito_client = create_cognito_client_from_config(config)
    cognito_tokens = cognito_client.get_tokens(
        user_name=config['COGNITO_USER_NAME'],
        password=config['COGNITO_USER_PASSWORD'],
    )
    return cognito_tokens.to_request_headers()


def create_cognito_client_from_config(config):
    cognito_client = create_boto_client('cognito-idp', region_name=config['AWS_REGION'])

    user_pool_id = config['COGNITO_USER_POOL_ID']
    app_client_id = config['COGNITO_APP_CLIENT_ID']
    app_client_secret = config['COGNITO_APP_CLIENT_SECRET']
    return CognitoClient(cognito_client, user_pool_id, app_client_id, app_client_secret)


@attr.s(frozen=True)
class AuthTokens:
    access_token: str = attr.ib(validator=instance_of(str))
    id_token: str = attr.ib(validator=instance_of(str))
    refresh_token: str = attr.ib(validator=instance_of(str))

    def to_request_headers(self):
        return {
            'Authorization': self.access_token,
            'X-Amazon-Cognito-Id': self.id_token
        }

    @staticmethod
    def from_boto_response(response):
        return AuthTokens(
            access_token=response['AuthenticationResult']['AccessToken'],
            id_token=response['AuthenticationResult']['IdToken'],
            refresh_token=response['AuthenticationResult']['RefreshToken']
        )


@attr.s(frozen=True)
class CognitoUser:
    name: str = attr.ib(validator=instance_of(str))
    password: str = attr.ib(validator=instance_of(str))


class CognitoClient:
    def __init__(self, client, user_pool_id, app_client_id, app_client_secret):
        self._user_pool_id = user_pool_id
        self._app_client_id = app_client_id
        self._app_client_secret = app_client_secret

        self._client = client

    def _create_secret_hash(self, user_name):
        message = user_name + self._app_client_id
        client_secret_bytes = bytes(self._app_client_secret, 'ascii')
        dig = hmac.new(client_secret_bytes, msg=message.encode('UTF-8'),
                       digestmod=hashlib.sha256).digest()
        return base64.b64encode(dig).decode()

    def create_user(self, user_name, password, groups):
        tmp_password = 'EyeUO^eueue3#!' * 3
        response = self._admin_create_user(user_name, tmp_password)
        assert response['User']['UserStatus'] == 'FORCE_CHANGE_PASSWORD', 'No password change enforced - user pool config changed?'

        response = self._initiate_auth(user_name, tmp_password)
        assert response['ChallengeName'] == 'NEW_PASSWORD_REQUIRED', 'No challenges after creation - user pool config changed?'
        response = self._set_up_password(response['Session'], user_name, password)
        assert 'AuthenticationResult' in response, 'Another challenge required after setting up password - user pool config changed?'

        self._add_to_groups(user_name, groups)

        logger.info(f'Created user {user_name} attached to {groups} groups')
        return CognitoUser(name=user_name, password=password)

    def delete_user(self, user_name):
        logger.info(f'Deleting user {user_name}')
        self._client.admin_delete_user(
            UserPoolId=self._user_pool_id,
            Username=user_name
        )

    @cached(cache=TTLCache(maxsize=1, ttl=50 * 60))
    def get_tokens(self, user_name, password) -> AuthTokens:
        response = self._initiate_auth(user_name, password)
        assert 'AuthenticationResult' in response, 'User creation was incorrect - some challenges are not passed'
        logger.debug(f'Got auth tokens for {user_name}')
        return AuthTokens.from_boto_response(response)

    def _admin_create_user(self, user_name, tmp_password):
        return self._client.admin_create_user(
            UserPoolId=self._user_pool_id,
            Username=user_name,
            TemporaryPassword=tmp_password
        )

    def _add_to_groups(self, user_name, groups):
        for group in groups:
            self._client.admin_add_user_to_group(
                UserPoolId=self._user_pool_id,
                Username=user_name,
                GroupName=group
            )

    @retry(stop_max_attempt_number=2, wait_fixed=2000)
    def _initiate_auth(self, user_name, password):
        logger.debug('Initiating the authentication flow for {user_name}')
        auth_parameters = {
            'USERNAME': user_name,
            'PASSWORD': password,
            'SECRET_HASH': self._create_secret_hash(user_name)
        }
        return self._client.initiate_auth(
            AuthFlow='USER_PASSWORD_AUTH',
            AuthParameters=auth_parameters,
            ClientId=self._app_client_id
        )

    @retry(stop_max_attempt_number=2, wait_fixed=2000)
    def _set_up_password(self, session, user_name, password):
        parameters = {
            'USERNAME': user_name,
            'NEW_PASSWORD': password,
            'SECRET_HASH': self._create_secret_hash(user_name)
        }
        return self._client.respond_to_auth_challenge(
            ChallengeName='NEW_PASSWORD_REQUIRED',
            ChallengeResponses=parameters,
            Session=session,
            ClientId=self._app_client_id
        )
