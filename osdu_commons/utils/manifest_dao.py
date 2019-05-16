import json

from osdu_commons.model.swps_manifest import manifest_from_camel_dict, SWPSManifest


class ManifestDao:
    def __init__(self, s3_client):
        self._s3_client = s3_client

    def load_manifest(self, s3_bucket: str, s3_key: str) -> SWPSManifest:
        response = self._s3_client.get_object(
            Bucket=s3_bucket,
            Key=s3_key,
        )
        manifest_dict = json.load(response['Body'])
        return manifest_from_camel_dict(manifest_dict)
