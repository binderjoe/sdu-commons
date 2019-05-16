import logging
import os
from typing import List, Dict, Iterable

import attr
from osdu_commons.clients.data_api_client import DataAPIClient, ResourceExists
from osdu_commons.model.aws import S3Location
from osdu_commons.model.enums import ResourceLifecycleStatus
from osdu_commons.model.file import ManifestFile
from osdu_commons.model.resource import Resource, ResourceInit, ResourceUpdate
from osdu_commons.model.smds_manifest import SMDSManifest
from osdu_commons.model.swps_manifest import SWPSManifest
from osdu_commons.model.work_product import WorkProductManifest
from osdu_commons.model.work_product_component import WorkProductComponentManifest
from osdu_commons.utils.srn import SRN

logger = logging.getLogger(__name__)


@attr.s(frozen=True, auto_attribs=True)
class CreateWorkProductResult:
    work_product: Resource
    file_associative_id_to_file_location_map: Dict[str, S3Location]


class DataAPIService:
    def __init__(self, data_api_client: DataAPIClient, region_id: SRN):
        self._data_api_client = data_api_client
        self._region_id = region_id

    @classmethod
    def from_environ(cls) -> 'DataAPIService':
        return cls(
            data_api_client=DataAPIClient(os.environ['DATA_API_BASE_URL']),
            region_id=SRN('reference-data/OSDURegion', os.environ['AWS_REGION'])
        )

    def create_smds_from_manifest(self, smds_manifest: SMDSManifest) -> SRN:

        try:
            resources = self._data_api_client.create_resources(
                resource_inits=[
                    ResourceInit(
                        type=smds_manifest.resource_type_id,
                        new_version=False,
                        key=smds_manifest.resource_id.detail
                    )
                ],
                region_id=self._region_id
            )
            resource_id = resources[0].id
        except ResourceExists:
            resources = self._data_api_client.create_resources(
                resource_inits=[
                    ResourceInit(
                        type=smds_manifest.resource_type_id,
                        new_version=True,
                        key=smds_manifest.resource_id.detail,
                        id=SRN(smds_manifest.resource_type_id.detail, smds_manifest.resource_id.detail),
                    )
                ],
                region_id=self._region_id
            )
            resource_id = resources[0].id

        self._data_api_client.update_resources(
            resource_updates=[
                ResourceUpdate(
                    id=resource_id,
                    data=smds_manifest.data,
                    lifecycle_status=ResourceLifecycleStatus.RECEIVED
                )
            ],
            region_id=self._region_id
        )

        return resource_id

    def create_work_product_from_manifest(self, manifest: SWPSManifest) -> CreateWorkProductResult:
        file_associative_id_to_resource_map = self._create_file_resources(
            file_definitions=manifest.files
        )
        work_product_component_resource_ids = self._create_work_product_component_resources(
            work_product_component_descriptions=manifest.work_product_components,
            associative_id_to_file_resource_id_map={k: v.id for k, v in file_associative_id_to_resource_map.items()}
        )
        work_product_resource = self._create_work_product(
            work_product_description=manifest.work_product,
            work_product_component_ids=work_product_component_resource_ids
        )

        return CreateWorkProductResult(
            work_product=work_product_resource,
            file_associative_id_to_file_location_map={
                k: v.s3_location for k, v in file_associative_id_to_resource_map.items()
            }
        )

    def iter_resources_tree(self, root_resource_id: SRN, with_artefacts: bool = False) -> Iterable[Resource]:
        root_resource = self.get_all_resources([root_resource_id])[0]
        yield root_resource
        yield from self._iter_resource_children(root_resource, with_artefacts)

    def _iter_resource_children(self, resource: Resource, with_artefacts: bool) -> Iterable[Resource]:
        is_work_product = resource.type_id.detail.startswith('work-product/')
        is_work_product_component = resource.type_id.detail.startswith('work-product-component/')
        has_children = is_work_product or is_work_product_component

        if has_children:
            group_type_properties = resource.data.get('GroupTypeProperties', {})
            if is_work_product:
                components_ids = [SRN.from_string(wpc) for wpc in group_type_properties.get('Components', [])]
                resources = self.get_all_resources(components_ids)
                yield from resources
                for r in resources:
                    yield from self._iter_resource_children(r, with_artefacts)

            elif is_work_product_component:
                files_ids = [SRN.from_string(f) for f in group_type_properties.get('Files', [])]
                yield from self.get_all_resources(files_ids)

            if with_artefacts:
                artefacts_ids = [SRN.from_string(f['ResourceID']) for f in group_type_properties.get('Artefacts', [])]
                yield from self.get_all_resources(artefacts_ids)

    def get_all_resources(self, resource_ids: List[SRN]) -> List[Resource]:
        def _recursive_get(ids, n=100):
            if n < 0:
                raise RuntimeError('Recursion limit exceeded when getting resources')

            all_resources = []
            get_resources_result = self._data_api_client.get_resources(resource_ids=ids)
            all_resources.extend(get_resources_result.resources)

            if len(get_resources_result.unprocessed_srns) > 0:
                all_resources.extend(
                    _recursive_get(get_resources_result.unprocessed_srns, n - 1)
                )

            return all_resources

        if not resource_ids:
            return []

        shuffled_resources = _recursive_get(resource_ids)
        id_to_resource_map = {resource.id.without_version: resource for resource in shuffled_resources}
        assert len(id_to_resource_map) == len(resource_ids), "IDs duplicated - two same SRNs with different versions?"
        resources_in_proper_order = [id_to_resource_map[id_.without_version] for id_ in resource_ids]

        return resources_in_proper_order

    def _create_file_resources(self, file_definitions: List[ManifestFile]) -> Dict[str, Resource]:
        file_resources = self._data_api_client.create_resources(
            resource_inits=[
                ResourceInit(
                    type=file_def.resource_type_id,
                    new_version=False,
                ) for file_def in file_definitions
            ],
            region_id=self._region_id
        )
        self._data_api_client.update_resources(
            resource_updates=[
                ResourceUpdate(
                    id=resource.id,
                    data=file_def.data.asdict(),
                    lifecycle_status=ResourceLifecycleStatus.LOADING
                ) for resource, file_def in zip(file_resources, file_definitions)
            ],
            region_id=self._region_id
        )
        file_resource_ids = [r.id for r in file_resources]
        logger.info(f'Created file resources: {file_resource_ids}')

        complete_resources = self.get_all_resources(resource_ids=file_resource_ids)
        return {
            file_def.associative_id: resource for file_def, resource in zip(file_definitions, complete_resources)
        }

    def _create_work_product_component_resources(
            self,
            work_product_component_descriptions: List[WorkProductComponentManifest],
            associative_id_to_file_resource_id_map: Dict[str, SRN]
    ) -> List[SRN]:
        wpc_resources = self._data_api_client.create_resources(
            resource_inits=[
                ResourceInit(
                    type=wpc_description.resource_type_id,
                    new_version=False,
                ) for wpc_description in work_product_component_descriptions
            ],
            region_id=self._region_id
        )

        wpc_id_to_new_data_map = {}
        for wpc_description, wpc_resource in zip(work_product_component_descriptions, wpc_resources):
            all_files = wpc_description.data.group_type_properties.files + [
                associative_id_to_file_resource_id_map[id_] for id_ in wpc_description.file_associative_ids
            ]
            new_data = attr.evolve(
                wpc_description.data,
                group_type_properties=attr.evolve(
                    wpc_description.data.group_type_properties,
                    files=all_files
                )
            )
            wpc_id_to_new_data_map[wpc_resource.id] = new_data

        self._data_api_client.update_resources(
            [
                ResourceUpdate(
                    id=id_,
                    data=data.asdict(),
                    lifecycle_status=ResourceLifecycleStatus.LOADING
                ) for id_, data in wpc_id_to_new_data_map.items()
            ], region_id=self._region_id
        )

        wpc_resource_ids = [r.id for r in wpc_resources]
        logger.info(f'Created WorkProductComponent resources: {wpc_resource_ids}')

        return wpc_resource_ids

    def _create_work_product(
            self,
            work_product_description: WorkProductManifest,
            work_product_component_ids: List[SRN]
    ) -> Resource:
        wp_resource = self._data_api_client.create_resources(
            resource_inits=[
                ResourceInit(
                    type=work_product_description.resource_type_id,
                    new_version=False,
                )
            ],
            region_id=self._region_id
        )[0]

        all_components = work_product_description.data.group_type_properties.components + work_product_component_ids
        new_data = attr.evolve(
            work_product_description.data,
            group_type_properties=attr.evolve(
                work_product_description.data.group_type_properties,
                components=all_components
            )
        )

        self._data_api_client.update_resources(
            [
                ResourceUpdate(
                    id=wp_resource.id,
                    data=new_data.asdict(),
                    lifecycle_status=ResourceLifecycleStatus.LOADING
                )
            ],
            region_id=self._region_id
        )

        logger.info(f'Created WorkProduct resource: {wp_resource.id}')

        complete_resource = self.get_all_resources(resource_ids=[wp_resource.id])[0]
        return complete_resource
