import pytest

from osdu_commons.clients.delivery_client import GetResourcesResponseSuccess, GetResourcesResponseNotFound, \
    GetResourcesException


def test_fetching_proper_resources(delivery_client, smds_srn):
    srns_to_fetch = [smds_srn]

    result = delivery_client.get_resources(srns_to_fetch)

    assert isinstance(result, GetResourcesResponseSuccess)
    assert len(result.result) == 1
    result_item = result.result[0]
    assert str(result_item.srn) == smds_srn


def test_fetching_wrong_srn(delivery_client):
    srn_to_fetch = ['123']

    with pytest.raises(GetResourcesException, match='Incorrect format of srn'):
        delivery_client.get_resources([srn_to_fetch])


def test_fetching_not_existing_resources(delivery_client):
    srn_to_fetch = 'srn:master-data/Well:12345:1'

    result = delivery_client.get_resources([srn_to_fetch])

    assert isinstance(result, GetResourcesResponseNotFound)
    assert len(result.not_found_resource_ids) == 1
    not_found_srn = result.not_found_resource_ids[0]
    assert str(not_found_srn) == srn_to_fetch
