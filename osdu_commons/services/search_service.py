from typing import Iterable

from osdu_commons.clients.search_client import SearchClient, SearchRequest, SearchResultFile


class SearchService:
    def __init__(self, search_client: SearchClient):
        self._search_client = search_client

    def get_search_files(self) -> Iterable[SearchResultFile]:
        request_input = SearchRequest(metadata={'resource_type': 'work-product-component*'})

        for search_response in self._search_client.iter_index_search(request_input):
            for search_result in search_response.results:
                yield from search_result.files
