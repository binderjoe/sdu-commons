import json

from osdu_commons.utils.manifest_dao import ManifestDao
from osdu_commons.utils.srn import SRN


def _save_file_to_s3(s3_client, json_data, bucket, key):
    json_binary_string = json.dumps(json_data).encode('utf-8')

    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(
        Body=json_binary_string,
        Bucket=bucket,
        Key=key
    )


def test_load_manifest(localstack_s3_client):
    manifest = {
        'WorkProduct': {
            'ResourceTypeId': 'srn:type:work-product/Test:',
            'ResourceSecurityClassification': 'srn:reference-data/ResourceSecurityClassification:MOST-CONFIDENTIAL:',
            'Data': {
                'GroupTypeProperties': {
                    'Components': []
                },
                'IndividualTypeProperties': {},
                'ExtensionProperties': {}
            },
            'ComponentsAssociativeIds': []
        },
        'WorkProductComponents': [],
        'files': []

    }
    bucket = 'bucket'
    key = 'key'
    _save_file_to_s3(localstack_s3_client, manifest, bucket, key)

    manifest_dao = ManifestDao(localstack_s3_client)
    loaded_manifest = manifest_dao.load_manifest(bucket, key)

    assert loaded_manifest.work_product.resource_type_id == SRN.from_string('srn:type:work-product/Test:')
