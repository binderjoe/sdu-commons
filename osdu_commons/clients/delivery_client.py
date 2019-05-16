import logging
from typing import List, Union, Optional

import attr
import requests
from attr.validators import instance_of, optional
from requests import Response

from osdu_commons.clients.cognito_aware_rest_client import CognitoAwareRestClient
from osdu_commons.clients.rest_client import HttpException, HttpNotFoundException
from osdu_commons.clients.retry import osdu_retry
from osdu_commons.model.aws import S3Location
from osdu_commons.utils import convert
from osdu_commons.utils.srn import SRN
from osdu_commons.utils.validators import list_of

MAX_RESOURCES_FETCHING_ATTEMPTS = 5

logger = logging.getLogger(__name__)


@attr.s(frozen=True)
class GetResourcesResultItem:
    srn: SRN = attr.ib(validator=instance_of(SRN), converter=convert.srn)
    data: Optional[dict] = attr.ib(validator=optional(instance_of(dict)), default=None)
    s3_location: Optional[S3Location] = attr.ib(
        validator=optional(instance_of(S3Location)),
        converter=attr.converters.optional(S3Location.convert),
        default=None)

    @staticmethod
    def convert(item):
        if isinstance(item, GetResourcesResultItem):
            return item

        return GetResourcesResultItem(
            srn=item['SRN'],
            data=item.get('Data'),
            s3_location=item.get('S3Location')
        )


@attr.s(frozen=True)
class GetResourcesResponseSuccess:
    result: List[GetResourcesResultItem] = attr.ib(
        validator=list_of(instance_of(GetResourcesResultItem)),
        converter=convert.list_(GetResourcesResultItem.convert))
    unprocessed_srn: List[SRN] = attr.ib(validator=list_of(instance_of(SRN)), converter=convert.list_(convert.srn))
    temporary_credentials: dict = attr.ib(validator=instance_of(dict))


@attr.s(frozen=True)
class GetResourcesResponseNotFound:
    not_found_resource_ids: List[SRN] = attr.ib(
        validator=list_of(instance_of(SRN)),
        converter=convert.list_(convert.srn))


class GetResourcesException(Exception):
    pass


class DeliveryClient(CognitoAwareRestClient):
    MAX_GET_RESOURCES_BATCH_SIZE = 100

    @osdu_retry()
    def _get_resources(self, srns_to_fetch: List[str], target_region_id: str) -> Response:
        logger.debug(f'Getting resources with srns: {srns_to_fetch}')
        return self.post(
            path='GetResources',
            json={
                'SRNS': srns_to_fetch,
                'TargetRegionID': target_region_id
            },
        )

    def get_resources(self, srns_to_fetch: List[SRN], target_region_id: str = 'srn:dummy:dummy:') \
            -> Union[GetResourcesResponseSuccess, GetResourcesResponseNotFound]:
        srns_to_fetch = [str(srn) for srn in srns_to_fetch]
        try:
            response = self._get_resources(srns_to_fetch, target_region_id)
            return self._handle_get_resources_200(response)
        except HttpNotFoundException as e:
            return self._handle_get_resources_404(e.response)
        except HttpException as e:
            raise GetResourcesException(e.response.text)

    @staticmethod
    def _handle_get_resources_200(response: requests.Response) -> GetResourcesResponseSuccess:
        response_json = response.json()
        temporary_credentials = response_json.get('TemporaryCredentials', {})

        return GetResourcesResponseSuccess(
            result=response_json['Result'],
            unprocessed_srn=response_json.get('UnprocessedSRNs', []),
            temporary_credentials=temporary_credentials
        )

    def _handle_get_resources_404(self, response: requests.Response) -> GetResourcesResponseNotFound:
        response_json = response.json()
        not_found_srns = self._parse_not_found_resources_error_msg(response_json['Error'])

        logger.info(f'Getting resources failed for {not_found_srns}')
        return GetResourcesResponseNotFound(
            not_found_resource_ids=not_found_srns,
        )

    @staticmethod
    def _parse_not_found_resources_error_msg(msg: dict) -> List[SRN]:
        return [SRN.from_string(srn) for srn in msg['NotFoundResourceIDs']]
