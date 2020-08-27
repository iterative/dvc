from functools import wraps

import boto3
import moto.s3.models as s3model
import pytest
from moto import mock_s3

from dvc.cache.base import CloudCache
from dvc.tree.s3 import S3Tree
from tests.remotes import S3

# from https://github.com/spulec/moto/blob/v1.3.5/tests/test_s3/test_s3.py#L40
REDUCED_PART_SIZE = 256


def reduced_min_part_size(f):
    """ speed up tests by temporarily making the multipart minimum part size
        small
    """
    orig_size = s3model.UPLOAD_PART_MIN_SIZE

    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            s3model.UPLOAD_PART_MIN_SIZE = REDUCED_PART_SIZE
            return f(*args, **kwargs)
        finally:
            s3model.UPLOAD_PART_MIN_SIZE = orig_size

    return wrapped


def _get_src_dst():
    base_info = S3Tree.PATH_CLS(S3.get_url())
    return base_info / "from", base_info / "to"


@mock_s3
def test_copy_singlepart_preserve_etag():
    from_info, to_info = _get_src_dst()

    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=from_info.bucket)
    s3.put_object(Bucket=from_info.bucket, Key=from_info.path, Body="data")

    S3Tree._copy(s3, from_info, to_info, {})


@mock_s3
@pytest.mark.parametrize(
    "base_info",
    [S3Tree.PATH_CLS("s3://bucket/"), S3Tree.PATH_CLS("s3://bucket/ns/")],
)
def test_link_created_on_non_nested_path(base_info, tmp_dir, dvc, scm):
    tree = S3Tree(dvc, {"url": str(base_info.parent)})
    cache = CloudCache(tree)
    s3 = cache.tree.s3.meta.client
    s3.create_bucket(Bucket=base_info.bucket)
    s3.put_object(
        Bucket=base_info.bucket, Key=(base_info / "from").path, Body="data"
    )
    cache.link(base_info / "from", base_info / "to")

    assert cache.tree.exists(base_info / "from")
    assert cache.tree.exists(base_info / "to")


@mock_s3
def test_makedirs_doesnot_try_on_top_level_paths(tmp_dir, dvc, scm):
    base_info = S3Tree.PATH_CLS("s3://bucket/")
    tree = S3Tree(dvc, {"url": str(base_info)})
    tree.makedirs(base_info)


def _upload_multipart(s3, Bucket, Key):
    mpu = s3.create_multipart_upload(Bucket=Bucket, Key=Key)
    mpu_id = mpu["UploadId"]

    parts = []
    n_parts = 10
    for i in range(1, n_parts + 1):
        # NOTE: Generation parts of variable size. Part size should be at
        # least 5MB:
        # https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
        part_size = REDUCED_PART_SIZE + i
        body = b"1" * part_size
        part = s3.upload_part(
            Bucket=Bucket,
            Key=Key,
            PartNumber=i,
            UploadId=mpu_id,
            Body=body,
            ContentLength=len(body),
        )
        parts.append({"PartNumber": i, "ETag": part["ETag"]})

    s3.complete_multipart_upload(
        Bucket=Bucket,
        Key=Key,
        UploadId=mpu_id,
        MultipartUpload={"Parts": parts},
    )


@mock_s3
@reduced_min_part_size
def test_copy_multipart_preserve_etag():
    from_info, to_info = _get_src_dst()

    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=from_info.bucket)
    _upload_multipart(s3, from_info.bucket, from_info.path)
    S3Tree._copy(s3, from_info, to_info, {})
