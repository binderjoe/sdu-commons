import logging
import time
from functools import partial
from itertools import islice
from typing import List, Optional, Iterable

import attr
from attr.validators import instance_of, optional
from pampy import match

from osdu_commons.clients.delivery_client import DeliveryClient, GetResourcesResponseSuccess, \
    GetResourcesResponseNotFound, GetResourcesResultItem
from osdu_commons.model.aws import S3Location
from osdu_commons.utils import convert
from osdu_commons.utils.srn import SRN
from osdu_commons.utils.validators import list_of

logger = logging.getLogger(__name__)

MAX_RESOURCES_FETCHING_ATTEMPTS = 5


@attr.s(frozen=True)
class DeliveredResource:
    srn: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    exists: bool = attr.ib(validator=instance_of(bool))
    data: Optional[dict] = attr.ib(validator=optional(instance_of(dict)), default=None)
    s3_location: Optional[S3Location] = attr.ib(validator=optional(instance_of(S3Location)), default=None)
    temporary_credentials: Optional[dict] = attr.ib(validator=optional(instance_of(dict)), default=None)

    @classmethod
    def from_json(cls, json_object, credentials, exists=True):
        return cls(
            srn=json_object['SRN'],
            data=json_object.get('Data'),
            s3_location=json_object.get('S3Location'),
            temporary_credentials=credentials,
            exists=exists
        )

    @classmethod
    def from_get_resource_result_item(cls, item: GetResourcesResultItem, credentials, exists=True):
        return cls(
            srn=item.srn,
            data=item.data,
            s3_location=item.s3_location,
            temporary_credentials=credentials,
            exists=exists,
        )


@attr.s(frozen=True)
class DeliveredResponse:
    delivery_resources: List[DeliveredResource] = attr.ib(validator=list_of(instance_of(DeliveredResource)))
    not_found_resources: List[DeliveredResource] = attr.ib(validator=list_of(instance_of(DeliveredResource)))
    unprocessed_srn: List[SRN] = attr.ib(validator=list_of(instance_of(SRN)))


class DeliveryServiceException(object):
    pass


class DeliveryService:
    MAX_GET_RESOURCES_BATCH_SIZE = 100

    def __init__(self, delivery_client: DeliveryClient):
        self._delivery_client = delivery_client

    def get_resources(self, resource_ids: Iterable[SRN]) -> Iterable[DeliveredResource]:
        resource_ids = iter(resource_ids)
        srns_to_fetch = list(islice(resource_ids, self.MAX_GET_RESOURCES_BATCH_SIZE))
        while len(srns_to_fetch) > 0:
            yield from self.get_resources_batch_unordered(srns_to_fetch)
            srns_to_fetch = list(islice(resource_ids, self.MAX_GET_RESOURCES_BATCH_SIZE))

    def get_resources_batch_unordered(self, resource_ids: List[SRN]) -> Iterable[DeliveredResource]:
        srns_to_fetch = set(resource_ids)
        for i in range(MAX_RESOURCES_FETCHING_ATTEMPTS):
            delivered_response = self.get_resources_batch_unordered_response(srns_to_fetch)
            yield from delivered_response.delivery_resources
            yield from delivered_response.not_found_resources

            srns_to_fetch = delivered_response.unprocessed_srn
            if len(srns_to_fetch) == 0:
                break
            logger.debug(f'Unprocessed srns: {delivered_response.unprocessed_srn} after {i} attempt')

            if i < MAX_RESOURCES_FETCHING_ATTEMPTS - 1:
                time.sleep(2 ** i)

        if len(srns_to_fetch) > 0:
            raise Exception(f'Cannot fetch srns: {srns_to_fetch}')

    def get_resources_batch_unordered_response(self, resource_ids: Iterable[SRN]) -> DeliveredResponse:
        srns_to_fetch = list(resource_ids)
        get_resources_response = self._delivery_client.get_resources(srns_to_fetch)

        return match(
            get_resources_response,
            GetResourcesResponseSuccess, self.handle_get_resources_success,
            GetResourcesResponseNotFound, partial(self.handle_get_resources_not_found, srn_to_fetch=srns_to_fetch),
        )

    @staticmethod
    def handle_get_resources_success(get_resources_response: GetResourcesResponseSuccess) -> DeliveredResponse:
        result = get_resources_response.result
        credentials = get_resources_response.temporary_credentials
        delivery_resources = [
            DeliveredResource.from_get_resource_result_item(res_item, credentials) for res_item in result]

        return DeliveredResponse(
            delivery_resources=delivery_resources,
            not_found_resources=[],
            unprocessed_srn=get_resources_response.unprocessed_srn
        )

    @staticmethod
    def handle_get_resources_not_found(get_resources_response: GetResourcesResponseNotFound,
                                       srn_to_fetch: List[SRN]) -> DeliveredResponse:
        not_found_srns = get_resources_response.not_found_resource_ids
        not_found_delivery_resources = [DeliveredResource(srn=srn, exists=False) for srn in not_found_srns]
        unprocessed_srn = list(set(srn_to_fetch) - set(not_found_srns))
        return DeliveredResponse(
            delivery_resources=[],
            not_found_resources=not_found_delivery_resources,
            unprocessed_srn=unprocessed_srn
        )

    def get_resource(self, resource_id: SRN) -> DeliveredResource:
        get_resources_result = list(self.get_resources([resource_id]))
        assert len(get_resources_result) == 1
        return get_resources_result[0]

    def check_if_resources_exist(self, resource_ids: Iterable[SRN]) -> bool:
        resources = self.get_resources(resource_ids)
        return all(resource.exists for resource in resources)

    def get_components_of_type(self, resource_id: SRN, component_type: str) -> Iterable[DeliveredResource]:
        resource = self.get_resource(resource_id)
        components_ids = [SRN.from_string(item) for item in resource.data['GroupTypeProperties']['Components']]
        components_ids_with_requested_type = [
            component_id for component_id in components_ids if component_id.type == component_type
        ]
        return self.get_resources(components_ids_with_requested_type)

    def get_component_of_type(self, resource_id: SRN, component_type: str) -> DeliveredResource:
        get_components_of_type_result = list(self.get_components_of_type(resource_id, component_type))
        assert len(get_components_of_type_result) == 1
        return get_components_of_type_result[0]
