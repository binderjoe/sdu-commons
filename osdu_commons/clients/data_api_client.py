import json
import logging
import os
from typing import List

import attr
from attr.validators import instance_of
from requests import Response

from osdu_commons.clients.rest_client import RestClient, HttpClientException
from osdu_commons.clients.retry import osdu_retry
from osdu_commons.model.aws import S3Location
from osdu_commons.model.resource import ResourceInit, Resource, ResourceUpdate
from osdu_commons.utils.srn import SRN
from osdu_commons.utils.validators import list_of

logger = logging.getLogger(__name__)


@attr.s(frozen=True)
class GetResourcesResult:
    resources: List[Resource] = attr.ib(validator=list_of(instance_of(Resource)))
    unprocessed_srns: List[SRN] = attr.ib(validator=list_of(instance_of(SRN)))


class DataAPIClient(RestClient):
    TIMEOUT = 7

    @classmethod
    def from_environ(cls) -> 'DataAPIClient':
        return cls(
            base_url=os.environ['DATA_API_BASE_URL']
        )

    @osdu_retry()
    def create_resources(self, resource_inits: List[ResourceInit], region_id: SRN) -> List[Resource]:
        types = [str(r.type) for r in resource_inits]

        logger.info(f'Creating resources of types: {types}')
        body = {
            'ResourceType': types,
            'NewVersion': [r.new_version for r in resource_inits],
            'RegionID': str(region_id),
            'Keys': [r.key for r in resource_inits],
            'ResourceIDs': [str(r.id) if r.id is not None else None for r in resource_inits],
        }
        try:
            response = self.post(body, path='v1/createresources')
        except HttpClientException as e:
            if e.http_status == 409:
                logger.exception(f'Resource exists (at least one existing key from {[r.key for r in resource_inits]})')
                raise ResourceExists() from e
            else:
                raise

        return self._get_resources_from_api_response(response)

    @osdu_retry()
    def update_resources(self, resource_updates: List[ResourceUpdate], region_id: SRN) -> List[Resource]:
        resource_ids = [str(r.id) for r in resource_updates]
        logger.info(f'Updating resources: {resource_ids}')

        resource_bodies = []
        for resource_update in resource_updates:
            resource_body = {}
            if resource_update.data is not None:
                resource_body['Data'] = resource_update.data
            if resource_update.curation_status is not None:
                resource_body['ResourceCurationStatus'] = str(resource_update.curation_status.value)
            if resource_update.lifecycle_status is not None:
                resource_body['ResourceLifecycleStatus'] = str(resource_update.lifecycle_status.value)
            resource_bodies.append(resource_body)

        body = {
            'ResourceIDs': resource_ids,
            'ResourceData': [json.dumps(resource_body) for resource_body in resource_bodies],
            'RegionID': str(region_id)
        }
        logger.debug(f'Updating {resource_ids} with {body}')
        response = self.post(body, path='v1/updateresources')

        old_resources = self._get_resources_from_api_response(response)
        return old_resources

    @osdu_retry()
    def get_resources(self, resource_ids: List[SRN]) -> GetResourcesResult:
        resource_ids = [str(rid) for rid in resource_ids]
        logger.info(f'Getting resources: {resource_ids}')

        body = {
            'ResourceIDs': resource_ids
        }
        response = self.post(body, path='v1/getresources')

        resources = self._get_resources_from_api_response(response)

        return GetResourcesResult(
            resources=resources,
            unprocessed_srns=[SRN.from_string(srn) for srn in response.json().get('UnprocessedSRNs', [])]
        )

    @staticmethod
    def _parse_data_api_location(data_api_s3_location: str) -> S3Location:
        data_api_location_split = data_api_s3_location[5:].split('/', 1)
        return S3Location(
            bucket=data_api_location_split[0],
            key=data_api_location_split[1]
        )

    def _get_resources_from_api_response(self, response: Response) -> List[Resource]:
        resources = []
        for i, resource_id in enumerate(response.json()['ResourceIDs']):
            resource_body = response.json()['ResourceData'][i]
            s3_locations = response.json().get('S3Location', [None] * len(response.json()['ResourceIDs']))

            resources.append(
                Resource(
                    id=resource_id,
                    type_id=resource_body['ResourceTypeID'],
                    home_region_id=resource_body['ResourceHomeRegionID'],
                    hosting_region_ids=resource_body['ResourceHostRegionIDs'],
                    object_creation_date_time=resource_body['ResourceObjectCreationDatetime'],
                    version_creation_date_time=resource_body['ResourceVersionCreationDatetime'],
                    curation_status=resource_body['ResourceCurationStatus'],
                    lifecycle_status=resource_body['ResourceLifecycleStatus'],
                    data=resource_body['Data'],
                    s3_location=self._parse_data_api_location(s3_locations[i]) if s3_locations[i] is not None else None
                )
            )
        return resources


class ResourceExists(Exception):
    pass
