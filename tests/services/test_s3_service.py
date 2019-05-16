import json
import time
from threading import Thread

import pytest

from osdu_commons.services.s3_service import CopySpecification, S3Service, S3Location

TEST_BUCKET_NAME = 'test_bucket'
TEST_FILE_NAME = 'some_test_file'
TEST_FILE_NAME_2 = 'some_test_file_2'
EXAMPLE_JSON_BODY = {'test': 1}


@pytest.fixture()
def test_bucket(localstack_s3_resource):
    bucket = localstack_s3_resource.create_bucket(Bucket=TEST_BUCKET_NAME)
    yield bucket
    bucket.objects.all().delete()
    bucket.delete()


@pytest.fixture()
def s3_service(localstack_s3_resource, localstack_s3_client):
    return S3Service(localstack_s3_resource, localstack_s3_client)


@pytest.fixture()
def example_file(test_bucket):
    test_bucket.put_object(Key=TEST_FILE_NAME, Body=json.dumps(EXAMPLE_JSON_BODY))
    return S3Location(test_bucket.name, TEST_FILE_NAME)


def test_load_json(s3_service, example_file):
    result = s3_service.load_json(example_file)
    assert result == EXAMPLE_JSON_BODY


def test_put_json(s3_service, example_file):
    data_to_put = [{'one': 1}, {'two': 2}]

    s3_service.put_json(example_file, data_to_put)

    result = s3_service.load_json(example_file)
    assert result == data_to_put


def test_copy(localstack_s3_client, s3_service, test_bucket, example_file):
    copy_spec_1 = CopySpecification(example_file, S3Location(test_bucket.name, 'first_destination'))
    copy_spec_2 = CopySpecification(example_file, S3Location(test_bucket.name, 'second_destination'))

    s3_service.copy([copy_spec_1, copy_spec_2])

    assert len(localstack_s3_client.list_objects(Bucket=test_bucket.name)['Contents']) == 3


def test_no_op_copy(localstack_s3_client, s3_service, test_bucket, example_file):
    s3_service.copy([])
    assert len(localstack_s3_client.list_objects(Bucket=test_bucket.name)['Contents']) == 1


def test_wait_passes_when_all_objects_already_exists(s3_service, example_file):
    locations = [example_file]
    s3_service.wait_for_object(locations)


def test_waiter_waits_when_none_object_exists(s3_service, test_bucket):
    locations = [S3Location(test_bucket.name, 'first'), S3Location(test_bucket.name, 'second')]
    finished = False

    def background_wait():
        nonlocal finished

        s3_service.wait_for_object(locations, delay_in_seconds=1)
        finished = True

    Thread(target=background_wait).start()

    time.sleep(1)
    assert finished is False
    test_bucket.Object('first').put()
    test_bucket.Object('second').put()
    time.sleep(1.5)
    assert finished


def test_waiter_waits_when_some_objects_exist(s3_service, test_bucket):
    locations = [S3Location(test_bucket.name, 'first'), S3Location(test_bucket.name, 'second')]
    test_bucket.Object('first').put()

    finished = False

    def background_wait():
        nonlocal finished

        s3_service.wait_for_object(locations, delay_in_seconds=1)
        finished = True

    Thread(target=background_wait).start()

    time.sleep(1)
    assert finished is False
    test_bucket.Object('second').put()

    time.sleep(1.5)
    assert finished


def test_get_presigned_url_return_presigned_url_post(s3_service, test_bucket):
    result = s3_service.generate_presigned_url(
        s3_location=S3Location(
            bucket=test_bucket.name,
            key='test_key'
        )
    )

    assert result.url.endswith(test_bucket.name)
    assert result.fields.key == 'test_key'
