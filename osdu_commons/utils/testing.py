import os
import random
import string
import uuid
from random import randint

from osdu_commons.clients.cognito_client import CognitoClient


def create_cognito_client(cognito_boto_client):
    assert 'COGNITO_USER_POOL_ID' in os.environ, 'Missing environment variable: COGNITO_USER_POOL_ID'
    assert 'COGNITO_APP_CLIENT_ID' in os.environ, 'Missing environment variable: COGNITO_APP_CLIENT_ID'
    assert 'COGNITO_APP_CLIENT_SECRET' in os.environ, 'Missing environment variable: COGNITO_APP_CLIENT_SECRET'
    user_pool_id = os.environ['COGNITO_USER_POOL_ID']
    app_client_id = os.environ['COGNITO_APP_CLIENT_ID']
    app_client_secret = os.environ['COGNITO_APP_CLIENT_SECRET']
    return CognitoClient(cognito_boto_client, user_pool_id, app_client_id, app_client_secret)


def create_cognito_user(cognito_client):
    user_name = 'TestUser_{}'.format(''.join(random.choices(string.ascii_uppercase + string.digits, k=10)))
    password = str(uuid.uuid4()) + 'Aa!1'
    return cognito_client.create_user(user_name, password, ['osdu_test_role'])


def create_cognito_headers(cognito_client, cognito_user):
    return cognito_client.get_tokens(cognito_user.name, cognito_user.password).to_request_headers()


def generate_random_smds_srn(data_type='Well'):
    srn_id = randint(10 ** 10, 10 ** 20)
    return f'srn:master-data/{data_type}:{srn_id}:1'
