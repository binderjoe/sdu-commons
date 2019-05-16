import json

import responses

from osdu_commons.clients.auth_client import AuthClient, AuthHeaders, AuthRequest

TEST_BASE_URL = 'https://example.com/'


@responses.activate
def test_create_empty_collection():
    auth_client = AuthClient(base_url=TEST_BASE_URL)
    event_headers = {'X-Amazon-Cognito-ID': 'cognito-123', 'Authorization': 'access-123'}
    auth_headers = AuthHeaders.from_event_headers(event_headers)
    auth_request = AuthRequest(action='some_action', method_arn='arn:123', service_name='test_service')

    responses.add(
        responses.POST,
        TEST_BASE_URL,
        json={'policy': json.dumps({'policy': 'policy-123'})},
        status=200
    )

    auth_response = auth_client.authorize(auth_request, auth_headers)

    assert len(responses.calls) == 1
    expected_headers = {
        'Authorization': 'access-123',
        'X-Amazon-Cognito-Id': 'cognito-123',
        'X-Resource-Names': ''
    }
    assert expected_headers.items() <= responses.calls[0].request.headers.items()
    assert auth_response.policy == {'policy': 'policy-123'}
