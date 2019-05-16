from osdu_commons.model.swps_manifest import manifest_from_camel_dict


def test_simple_manifest(example_manifest):
    manifest_from_camel_dict(example_manifest)
