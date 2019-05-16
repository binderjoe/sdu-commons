from typing import List

import attr
from attr.validators import instance_of

from osdu_commons.model.file import ManifestFile
from osdu_commons.model.work_product import WorkProductManifest
from osdu_commons.model.work_product_component import WorkProductComponentManifest
from osdu_commons.utils import convert
from osdu_commons.utils.validators import list_of


@attr.s(frozen=True)
class SWPSManifest:
    work_product: WorkProductManifest = attr.ib(
        converter=convert.class_from_camel_dict(WorkProductManifest))
    work_product_components: List[WorkProductComponentManifest] = attr.ib(
        validator=list_of(instance_of(WorkProductComponentManifest)),
        converter=convert.list_(convert.class_from_camel_dict(WorkProductComponentManifest)))
    files: List[ManifestFile] = attr.ib(
        validator=list_of(instance_of(ManifestFile)),
        converter=convert.list_(convert.class_from_camel_dict(ManifestFile)))

    def asdict(self):
        return {
            'WorkProduct': self.work_product.asdict(),
            'WorkProductComponents': [wpc.asdict() for wpc in self.work_product_components],
            'Files': [f.asdict() for f in self.files],
        }


def manifest_from_camel_dict(camel_dict: dict) -> SWPSManifest:
    return convert.class_from_camel_dict(SWPSManifest)(camel_dict)
