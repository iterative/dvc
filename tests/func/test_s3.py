from functools import wraps

import boto3
import moto.s3.models as s3model
from moto import mock_s3

from dvc.remote.s3 import RemoteS3
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
    base_info = RemoteS3.path_cls(S3.get_url())
    return base_info / "from", base_info / "to"


@mock_s3
def test_copy_singlepart_preserve_etag():
    from_info, to_info = _get_src_dst()

    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=from_info.bucket)
    s3.put_object(Bucket=from_info.bucket, Key=from_info.path, Body="data")

    RemoteS3._copy(s3, from_info, to_info, {})


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
    RemoteS3._copy(s3, from_info, to_info, {})
