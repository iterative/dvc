import pytest

from dvc.config import ConfigError
from dvc.exceptions import DvcException
from dvc.tree.s3 import S3Tree

bucket_name = "bucket-name"
prefix = "some/prefix"
url = f"s3://{bucket_name}/{prefix}"
key_id = "key-id"
key_secret = "key-secret"
session_token = "session-token"


@pytest.fixture(autouse=True)
def grants():
    return {
        "grant_read": "id=read-permission-id,id=other-read-permission-id",
        "grant_read_acp": "id=read-acp-permission-id",
        "grant_write_acp": "id=write-acp-permission-id",
        "grant_full_control": "id=full-control-permission-id",
    }


def test_init(dvc):
    config = {"url": url}
    tree = S3Tree(dvc, config)

    assert tree.path_info == url


def test_grants(dvc):
    config = {
        "url": url,
        "grant_read": "id=read-permission-id,id=other-read-permission-id",
        "grant_read_acp": "id=read-acp-permission-id",
        "grant_write_acp": "id=write-acp-permission-id",
        "grant_full_control": "id=full-control-permission-id",
    }
    tree = S3Tree(dvc, config)

    assert (
        tree.extra_args["GrantRead"]
        == "id=read-permission-id,id=other-read-permission-id"
    )
    assert tree.extra_args["GrantReadACP"] == "id=read-acp-permission-id"
    assert tree.extra_args["GrantWriteACP"] == "id=write-acp-permission-id"
    assert (
        tree.extra_args["GrantFullControl"] == "id=full-control-permission-id"
    )


def test_grants_mutually_exclusive_acl_error(dvc, grants):
    for grant_option, grant_value in grants.items():
        config = {"url": url, "acl": "public-read", grant_option: grant_value}

        with pytest.raises(ConfigError):
            S3Tree(dvc, config)


def test_sse_kms_key_id(dvc):
    tree = S3Tree(dvc, {"url": url, "sse_kms_key_id": "key"})
    assert tree.extra_args["SSEKMSKeyId"] == "key"


def test_key_id_and_secret(dvc):
    tree = S3Tree(
        dvc,
        {
            "url": url,
            "access_key_id": key_id,
            "secret_access_key": key_secret,
            "session_token": session_token,
        },
    )
    assert tree.access_key_id == key_id
    assert tree.secret_access_key == key_secret
    assert tree.session_token == session_token


def test_get_s3_no_credentials(mocker):
    from botocore.exceptions import NoCredentialsError

    tree = S3Tree(None, {})
    with pytest.raises(DvcException, match="Unable to find AWS credentials"):
        with tree._get_s3():
            raise NoCredentialsError


def test_get_s3_connection_error(mocker):
    from botocore.exceptions import EndpointConnectionError

    tree = S3Tree(None, {})

    msg = "Unable to connect to 'AWS S3'."
    with pytest.raises(DvcException, match=msg):
        with tree._get_s3():
            raise EndpointConnectionError(endpoint_url="url")


def test_get_s3_connection_error_endpoint(mocker):
    from botocore.exceptions import EndpointConnectionError

    tree = S3Tree(None, {"endpointurl": "https://example.com"})

    msg = "Unable to connect to 'https://example.com'."
    with pytest.raises(DvcException, match=msg):
        with tree._get_s3():
            raise EndpointConnectionError(endpoint_url="url")


def test_get_bucket():
    tree = S3Tree(None, {"url": "s3://mybucket/path"})
    with pytest.raises(DvcException, match="Bucket 'mybucket' does not exist"):
        with tree._get_bucket("mybucket") as bucket:
            raise bucket.meta.client.exceptions.NoSuchBucket({}, None)
