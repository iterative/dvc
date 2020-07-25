import pytest

from dvc.config import ConfigError
from dvc.tree.s3 import S3Tree

bucket_name = "bucket-name"
prefix = "some/prefix"
url = f"s3://{bucket_name}/{prefix}"
key_id = "key-id"
key_secret = "key-secret"


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
        {"url": url, "access_key_id": key_id, "secret_access_key": key_secret},
    )
    assert tree.access_key_id == key_id
    assert tree.secret_access_key == key_secret
