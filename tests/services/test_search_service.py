from unittest.mock import Mock

from osdu_commons.clients.search_client import SearchResponse, SearchResult, SearchResultFile
from osdu_commons.services.search_service import SearchService


def create_search_client_mock(search_responses):
    search_client_mock = Mock()
    iter_index_search_mock = Mock(return_value=iter(search_responses))
    search_client_mock.iter_index_search = iter_index_search_mock
    return search_client_mock


def test_search_osdu_files(search_service: SearchService):
    file1, file2, file3 = (Mock(spec=SearchResultFile) for _ in range(3))
    search_responses = [
        SearchResponse(results=[SearchResult(files=[file1, file2], srn='srn:a:b:1', data={})], total_hits=3, facets={},
                       start=0, count=2),
        SearchResponse(results=[SearchResult(files=[file3], srn='srn:c:d:1', data={})], total_hits=3, facets={},
                       start=2, count=1),
    ]
    search_service._search_client = create_search_client_mock(search_responses)

    search_files = list(search_service.get_search_files())

    assert search_files == [file1, file2, file3]
