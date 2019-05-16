import responses
from osdu_commons.clients.collection_client import CollectionClient, Collection
from osdu_commons.utils.srn import SRN
from tests.test_root import TEST_COLLECTION_BASE_URL


@responses.activate
def test_create_empty_collection(collection_client: CollectionClient):
    srn = 'srn:collection/abc:1:1'
    responses.add(
        responses.POST,
        f'{TEST_COLLECTION_BASE_URL}/CreateCollection',
        json={
            'SRN': srn
        },
        status=200
    )
    create_collection_response = collection_client.create_collection('owner', 'name')
    assert len(responses.calls) == 1
    assert create_collection_response == SRN.from_string(srn)


@responses.activate
def test_update_collection(collection_client: CollectionClient):
    collection_srn = 'srn:collection/abc:1:1'
    resource_srn = 'srn:file/abc:1:1'
    responses.add(
        responses.POST,
        f'{TEST_COLLECTION_BASE_URL}/UpdateCollection',
        json={
            'SRN': collection_srn
        },
        status=200
    )
    update_collection_response = collection_client.update_collection(
        collection_srn,
        'owner',
        'name',
        resources=[SRN.from_string(resource_srn)]
    )
    assert len(responses.calls) == 1
    assert update_collection_response == SRN.from_string(collection_srn)


@responses.activate
def test_get_collection(collection_client: CollectionClient):
    collection_srn = SRN.from_string('srn:collection/abc:1:1')
    resource_srn = 'srn:file/abc:1:1'
    responses.add(
        responses.POST,
        f'{TEST_COLLECTION_BASE_URL}/GetCollection',
        json={
            'SRN': str(collection_srn),
            'OwnerId': 'owner',
            'name': 'collection name',
            'resources': [resource_srn]
        },
        status=200
    )
    get_collection_response = collection_client.get_collection(collection_srn)
    assert len(responses.calls) == 1
    assert isinstance(get_collection_response, Collection)
    assert get_collection_response.srn == collection_srn


@responses.activate
def test_list_collections(collection_client: CollectionClient):
    collection_srn = 'srn:collection/abc:1:1'
    resource_srn = 'srn:file/abc:1:1'
    responses.add(
        responses.POST,
        f'{TEST_COLLECTION_BASE_URL}/ListCollection',
        json={
            'nextToken': None,
            'collections': [
                {
                    'SRN': collection_srn,
                    'OwnerId': 'owner',
                    'name': 'collection name',
                    'resources': [resource_srn]
                }
            ]
        },
        status=200
    )
    list_collections_response = collection_client.list_collections('owner')
    assert len(responses.calls) == 1
    assert len(list_collections_response) == 1
    assert isinstance(list_collections_response[0], Collection)


@responses.activate
def test_delete_collection(collection_client: CollectionClient):
    collection_srn = SRN.from_string('srn:collection/abc:1:1')
    responses.add(
        responses.POST,
        f'{TEST_COLLECTION_BASE_URL}/DeleteCollection',
        json={},
        status=200
    )
    collection_client.delete_collection(collection_srn)
    assert len(responses.calls) == 1
