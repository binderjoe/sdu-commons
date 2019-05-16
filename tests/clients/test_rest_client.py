import json

import pytest
import responses

from osdu_commons.clients.rest_client import (HttpClientException, HttpServerException, HttpUnrecognizedException,
                                              RestClient)

TEST_BASE_URL = 'https://example.com'


@pytest.fixture()
def rest_client():
    return RestClient(base_url=TEST_BASE_URL)


@responses.activate
def test_post_successful(rest_client):
    responses.add(
        responses.POST,
        f'{TEST_BASE_URL}/Test',
        json={},
        status=200
    )

    rest_client.post(
        json={'a': 'b'},
        path=f'{TEST_BASE_URL}/Test',
        params={'c': 'd'},
        headers={'user-agent': 'test-test'}
    )

    assert len(responses.calls) == 1
    call = responses.calls[0]
    assert json.loads(call.request.body) == {'a': 'b'}
    assert call.request.path_url == '/Test?c=d'
    assert call.request.headers['user-agent'] == 'test-test'


@responses.activate
def test_post_400(rest_client):
    responses.add(
        responses.POST,
        f'{TEST_BASE_URL}/Test',
        json={},
        status=400
    )

    with pytest.raises(HttpClientException):
        rest_client.post(
            json={'a': 'b'},
            path=f'{TEST_BASE_URL}/Test',
        )


@responses.activate
def test_post_500(rest_client):
    responses.add(
        responses.POST,
        f'{TEST_BASE_URL}/Test',
        json={},
        status=500
    )

    with pytest.raises(HttpServerException):
        rest_client.post(
            json={'a': 'b'},
            path=f'{TEST_BASE_URL}/Test',
        )


@responses.activate
def test_post_raises_unrecognized_exception(rest_client):
    responses.add(
        responses.POST,
        f'{TEST_BASE_URL}/Test',
        json={},
        status=999
    )

    with pytest.raises(HttpUnrecognizedException):
        rest_client.post(
            json={'a': 'b'},
            path=f'{TEST_BASE_URL}/Test',
        )
