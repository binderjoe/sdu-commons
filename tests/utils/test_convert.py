from osdu_commons.model.swps_manifest import manifest_from_camel_dict


def test_manifest_as_dict_preserves_same_structure(example_manifest):
    manifest_obj = manifest_from_camel_dict(example_manifest)
    manifest_back_to_json = manifest_obj.asdict()
    assert manifest_back_to_json == example_manifest
