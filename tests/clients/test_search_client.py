import pytest
import responses

from osdu_commons.clients.search_client import (SearchClient, SearchRequest, SearchResponse, SearchResult,
                                                SearchResultFile)
from tests.test_root import TEST_SEARCH_SERVICE_BASE_URL


@pytest.mark.parametrize('search_results', [
    [],
    [SearchResult(files=[SearchResultFile('filename', 'srn:a:b:1')], srn='srn:c:d:1', data={})],
    [SearchResult(files=[SearchResultFile('filename', 'srn:a:b:1')], srn='srn:c:d:1',
                  data={'geo_location': {'a': 1},
                        'survey_boundary': {'b': 2},
                        'live_traces_boundary': {'c': 3}
                        })],
    [
        SearchResult(files=[SearchResultFile('filename1', 'srn:a:b:1')], srn='srn:c:d:1', data={}),
        SearchResult(files=[SearchResultFile('filename2', 'srn:a:b:2')], srn='srn:c:d:2', data={}),
        SearchResult(files=[SearchResultFile('filename3', 'srn:a:b:2')], srn='srn:c:d:3', data={}),
    ]
])
@responses.activate
def test_index_search(search_results, search_client: SearchClient):
    facets = {'a': 1}
    expected_search_response = SearchResponse(results=search_results, total_hits=1, facets=facets, start=0, count=1)
    responses.add(
        responses.POST,
        f'{TEST_SEARCH_SERVICE_BASE_URL}/indexSearch',
        json={
            'results': [result.asdict() for result in search_results],
            'start': 0,
            'count': 1,
            'total_hits': 1,
            'facets': facets
        },
        status=200
    )

    search_request = SearchRequest(
        metadata = { 'test' : 123},
        fulltext='one',
        geo_location = {
            'type': 'polygon',
            'coordinates': [[
                [-109.05029296875, 37.00255267215955],
                [-102.01904296874999, 37.00255267215955],
                [ -102.01904296874999, 41.02964338716638],
                [-109.05029296875,41.02964338716638],
                [-109.05029296875,37.00255267215955]
            ]]
        },    
        geo_centroid = [
            [-109.05029296875, 37.00255267215955],
            [-102.01904296874999, 37.00255267215955],
            [ -102.01904296874999, 41.02964338716638],
            [-109.05029296875,41.02964338716638],
            [-109.05029296875,37.00255267215955]
        ],
        start=0, count=0)  # values will be ignored
    search_response = search_client.index_search(search_request)

    assert len(responses.calls) == 1
    assert search_response == expected_search_response


@responses.activate
def test_index_search_fields_are_optional(search_client: SearchClient):
    expected_search_response = SearchResponse(results=[], total_hits=0, facets={}, start=0, count=0)
    responses.add(
        responses.POST,
        f'{TEST_SEARCH_SERVICE_BASE_URL}/indexSearch',
        json={
            'results': [],
            'start': 0,
            'count': 0,
            'total_hits': 0,
            'facets': {}
        },
        status=200
    )

    search_request = SearchRequest(
        start=0, count=0)  # values will be ignored
    search_response = search_client.index_search(search_request)

    assert len(responses.calls) == 1
    assert search_response == expected_search_response


@responses.activate
def test_iter_index_search_multiple_call(search_client: SearchClient):
    facets = {'a': 1}
    all_search_results = [
        SearchResult(files=[SearchResultFile('filename1', 'srn:a:b:1')], srn='srn:c:d:1', data={}),
        SearchResult(files=[SearchResultFile('filename2', 'srn:a:b:2')], srn='srn:c:d:2', data={}),
        SearchResult(files=[SearchResultFile('filename3', 'srn:a:b:2')], srn='srn:c:d:3', data={}),
    ]
    responses.add(
        responses.POST,
        f'{TEST_SEARCH_SERVICE_BASE_URL}/indexSearch',
        json={
            'results': [result.asdict() for result in all_search_results[:2]],
            'start': 0,
            'count': 2,
            'total_hits': 3,
            'facets': facets
        },
        status=200
    )
    responses.add(
        responses.POST,
        f'{TEST_SEARCH_SERVICE_BASE_URL}/indexSearch',
        json={
            'results': [result.asdict() for result in all_search_results[2:]],
            'start': 2,
            'count': 1,
            'total_hits': 3,
            'facets': facets
        },
        status=200
    )

    search_request = SearchRequest(metadata={}, start=0, count=0)  # values will be ignored
    search_results = list(search_client.iter_index_search(search_request))

    assert len(responses.calls) == 2
    assert search_results == all_search_results


def test_search_result_from_json():
    data_from_search_service = {
        'curves': [
            {'mnemonic': 'nzuimqBUTYdnLbbJ4E3i'}
        ],
        'geo_location': {
            'coordinates': [-32.0, -6.0],
            'type': 'point'
        },
        'geo_centroid': [[-32.0, -6.0]],
        'well_bore_srn': ['srn:master-data/Wellbore:2a895ff6ecc84adfafdb5d0fa6263cc3:'],
        'resource_type': 'master-data/Well',
        'well_log_srn': ['srn:work-product-component/WellLog:1d511efbba4d4c90ba255140ead07933:1'],
        'files': [
            {'filename': 'md2log1.WP16001609262018l90k',
             'srn': 'srn:file/md2log:1234567890:1'}
        ],
        'well_log_count': 1,
        'srn': 'srn:master-data/Well:1234567890:',
        'well_bore_count': 1
    }
    result = SearchResult.converter(data_from_search_service)
    assert result.srn == 'srn:master-data/Well:1234567890:'
    assert result.files == [SearchResultFile(filename='md2log1.WP16001609262018l90k',
                                             srn='srn:file/md2log:1234567890:1')]
    assert result.data['curves'][0]['mnemonic'] == 'nzuimqBUTYdnLbbJ4E3i'
    assert result.data['well_log_count'] == 1
    assert result.asdict() == data_from_search_service

    result_from_as_dict = SearchResult.converter(result.asdict())
    assert result_from_as_dict == result
