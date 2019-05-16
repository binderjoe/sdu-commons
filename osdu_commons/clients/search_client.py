import logging
from typing import Iterable, List, Optional, Dict

import attr
from attr.validators import instance_of, optional

from osdu_commons.clients.cognito_aware_rest_client import CognitoAwareRestClient
from osdu_commons.clients.retry import osdu_retry
from osdu_commons.utils import convert
from osdu_commons.utils.validators import list_of

logger = logging.getLogger(__name__)

MAX_REQUEST_COUNT = 100


@attr.s(frozen=True)
class SearchResultFile:
    filename: str = attr.ib(validator=instance_of(str))
    srn: str = attr.ib(validator=instance_of(str))

    @classmethod
    def converter(cls, item):
        if isinstance(item, SearchResultFile):
            return item
        return cls(item['filename'], item['srn'])

    def asdict(self):
        return {
            'filename': self.filename,
            'srn': self.srn
        }


@attr.s(frozen=True)
class SearchResult:
    files: List[SearchResultFile] = attr.ib(
        validator=list_of(instance_of(SearchResultFile)),
        converter=convert.list_(SearchResultFile.converter)
    )
    srn: str = attr.ib(validator=instance_of(str))
    data: Dict = attr.ib(validator=instance_of(Dict))

    @classmethod
    def converter(cls, item):
        if isinstance(item, SearchResult):
            return item
        data = item.copy()
        files = data.pop('files', [])
        srn = data.pop('srn')
        return cls(files=files, srn=srn, data=data)

    def asdict(self):
        result = self.data.copy()
        result['srn'] = self.srn
        result['files'] = [f.asdict() for f in self.files]
        return result


@attr.s(frozen=True)
class SearchResponse:
    results: List[SearchResult] = attr.ib(
        validator=list_of(instance_of(SearchResult)),
        converter=convert.list_(SearchResult.converter)
    )
    total_hits: int = attr.ib(validator=instance_of(int), converter=int)
    facets: dict = attr.ib(validator=instance_of(dict), converter=convert.copy)
    start: int = attr.ib(validator=instance_of(int), converter=int)
    count: int = attr.ib(validator=instance_of(int), converter=int)

    @property
    def has_results_left(self):
        return self.end < self.total_hits

    @property
    def end(self):
        return self.start + self.count

    def __str__(self):
        return f'SearchResponse stat: <start: {self.start}, count: {self.count}, number: {len(self.results)}, ' \
            f'total {self.total_hits}>'


@attr.s()
class SearchRequest:
    metadata: Optional[Dict] = attr.ib(validator=optional(instance_of(Dict)), converter=convert.copy, default=None)
    geo_location: Optional[Dict] = attr.ib(validator=optional(instance_of(Dict)), converter=convert.copy, default=None)
    geo_centroid: Optional[List[List[float]]] = attr.ib(
        validator=optional(list_of(list_of(instance_of(float)))),
        converter=attr.converters.optional(convert.list_(convert.list_())),
        factory=list)
    fulltext: Optional[str] = attr.ib(
        validator=optional(instance_of(str)),
        default=None)
    start: int = attr.ib(validator=instance_of(int), converter=int, default=0)
    count: int = attr.ib(validator=instance_of(int), converter=int, default=MAX_REQUEST_COUNT)
    map_aggregates: Optional[bool] = attr.ib(validator=optional(instance_of(bool)), default=None)
    full_results: Optional[bool] = attr.ib(validator=optional(instance_of(bool)), default=None)
    facets: Optional[List[str]] = attr.ib(validator=optional(list_of(instance_of(str))), default=None)
    aggregates_count: Optional[int] = attr.ib(validator=optional(instance_of(int)), default=None)
    sort: Optional[List[dict]] = attr.ib(validator=optional(list_of(instance_of(dict))), default=None)

    def asdict(self):
        return {
            'metadata': self.metadata,
            'geo_location': self.geo_location,
            'geo_centroid': self.geo_centroid,
            'fulltext': self.fulltext,
            'start': self.start,
            'count': self.count,
            'map_aggregates': self.map_aggregates,
            'full_results': self.full_results,
            'facets': self.facets,
            'aggregates_count': self.aggregates_count,
            'sort': self.sort,
        }


class SearchClient(CognitoAwareRestClient):
    SEARCH_CLIENT_MAX_RETRIES = 5

    @osdu_retry()
    def index_search(self, search_request: SearchRequest) -> SearchResponse:
        logger.debug(f'Searching for {search_request.asdict()}')
        response = self.post(
            path='indexSearch',
            json=search_request.asdict(),
            headers=self._cognito_headers,
        )

        return SearchResponse(**response.json())

    def iter_index_search(self, search_request: SearchRequest) -> Iterable[SearchResult]:
        search_response = SearchResponse(results=[], total_hits=1, facets={}, start=0, count=0)

        while search_response.has_results_left:
            search_request.start = search_response.end

            search_response = self.index_search(search_request)
            logger.debug(search_response)
            yield from search_response.results
