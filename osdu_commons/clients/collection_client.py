import logging
from typing import List, Optional

import attr
from attr.validators import instance_of, optional

from osdu_commons.clients.cognito_aware_rest_client import CognitoAwareRestClient
from osdu_commons.clients.retry import osdu_retry
from osdu_commons.utils.srn import SRN
from osdu_commons.utils.validators import list_of

logger = logging.getLogger(__name__)


@attr.s(frozen=True)
class Collection:
    srn: SRN = attr.ib(validator=instance_of(SRN))
    owner_id: Optional[str] = attr.ib(validator=optional(instance_of(str)))
    name: Optional[str] = attr.ib(validator=optional(instance_of(str)))
    description: Optional[str] = attr.ib(validator=optional(instance_of(str)))
    workspace_srn: Optional[SRN] = attr.ib(validator=optional(instance_of(SRN)))
    resources: List[str] = attr.ib(validator=list_of(instance_of(SRN)))
    filter_specification: List[dict] = attr.ib(validator=list_of(dict))

    @classmethod
    def from_json(cls, json_object):
        return cls(
            srn=SRN.from_string(json_object['SRN']),
            owner_id=json_object.get('OwnerID'),
            name=json_object.get('Name'),
            description=json_object.get('Description'),
            workspace_srn=(
                SRN.from_string(json_object['WorkSpaceSRN'])
                if json_object.get('WorkSpaceSRN') else None
            ),
            resources=[SRN.from_string(srn) for srn in json_object.get('Resources', [])],
            filter_specification=json_object.get('FilterSpecification', [])
        )


class CollectionClient(CognitoAwareRestClient):

    @osdu_retry()
    def create_collection(self, owner_id: str, name: str, description: str = None,
                          workspace_srn: SRN = None, resources: List[SRN] = None,
                          filter_specification: List[dict] = None) -> SRN:
        logger.info(f'Create collection {name} with owner {owner_id}')
        response_json = self.post(
            path='CreateCollection',
            json={
                'OwnerID': owner_id,
                'Name': name,
                'Description': description,
                'WorkSpaceSRN': str(workspace_srn) if workspace_srn else None,
                'Resources': [str(srn) for srn in resources] if resources is not None else None,
                'FilterSpecification': filter_specification
            }
        ).json()

        return SRN.from_string(response_json['SRN'])

    @osdu_retry()
    def update_collection(self, collection_srn: SRN, owner_id: str, name: str,
                          description: str = None, workspace_srn: SRN = None,
                          resources: List[SRN] = None, filter_specification: List[dict] = None) -> SRN:
        logger.info(f'Update collection {str(collection_srn)}')
        response_json = self.post(
            path='UpdateCollection',
            json={
                'SRN': str(collection_srn),
                'OwnerID': owner_id,
                'Name': name,
                'Description': description,
                'WorkSpaceSRN': str(workspace_srn) if workspace_srn else None,
                'Resources': [str(srn) for srn in resources] if resources is not None else None,
                'FilterSpecification': filter_specification
            }
        ).json()

        return SRN.from_string(response_json['SRN'])

    @osdu_retry()
    def get_collection(self, collection_srn: SRN) -> Collection:
        response_json = self.post(
            path='GetCollection',
            json={
                'SRN': str(collection_srn)
            }
        ).json()

        return Collection.from_json(response_json)

    @osdu_retry()
    def list_collections(self, owner_id: str) -> List[Collection]:
        # pagination is not yet implemented in Collection Service
        response_json = self.post(
            path='ListCollection',
            json={
                'OwnerID': owner_id
            }
        ).json()
        collections = response_json['collections']
        return [Collection.from_json(collection) for collection in collections]

    @osdu_retry()
    def delete_collection(self, collection_srn: SRN) -> None:
        logger.info(f'Delete collection {str(collection_srn)}')
        self.post(
            path='DeleteCollection',
            json={
                'SRN': str(collection_srn)
            }
        )
