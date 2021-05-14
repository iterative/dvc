import importlib
import sys
import textwrap
from functools import wraps

import boto3
import moto.s3.models as s3model
import pytest
from moto import mock_s3

from dvc.fs.s3 import S3FileSystem
from dvc.objects.db.base import ObjectDB
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
    base_info = S3FileSystem.PATH_CLS(S3.get_url())
    return base_info / "from", base_info / "to"


@mock_s3
def test_copy_singlepart_preserve_etag():
    from_info, to_info = _get_src_dst()

    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=from_info.bucket)
    s3.put_object(Bucket=from_info.bucket, Key=from_info.path, Body="data")

    S3FileSystem._copy(s3, from_info, to_info, {})


@mock_s3
@pytest.mark.parametrize(
    "base_info",
    [
        S3FileSystem.PATH_CLS("s3://bucket/"),
        S3FileSystem.PATH_CLS("s3://bucket/ns/"),
    ],
)
def test_link_created_on_non_nested_path(base_info, tmp_dir, dvc, scm):
    from dvc.checkout import _link

    fs = S3FileSystem(dvc, {"url": str(base_info.parent)})
    odb = ObjectDB(fs)
    s3 = odb.fs.s3.meta.client
    s3.create_bucket(Bucket=base_info.bucket)
    s3.put_object(
        Bucket=base_info.bucket, Key=(base_info / "from").path, Body="data"
    )
    _link(odb, base_info / "from", base_info / "to")

    assert odb.fs.exists(base_info / "from")
    assert odb.fs.exists(base_info / "to")


@mock_s3
def test_makedirs_doesnot_try_on_top_level_paths(tmp_dir, dvc, scm):
    base_info = S3FileSystem.PATH_CLS("s3://bucket/")
    fs = S3FileSystem(dvc, {"url": str(base_info)})
    fs.makedirs(base_info)


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
    S3FileSystem._copy(s3, from_info, to_info, {})


def test_s3_isdir(tmp_dir, dvc, s3):
    s3.gen({"data": {"foo": "foo"}})
    fs = S3FileSystem(dvc, s3.config)

    assert not fs.isdir(s3 / "data" / "foo")
    assert fs.isdir(s3 / "data")


def test_s3_upload_fobj(tmp_dir, dvc, s3):
    s3.gen({"data": {"foo": "foo"}})
    fs = S3FileSystem(dvc, s3.config)

    to_info = s3 / "data" / "bar"
    with fs.open(s3 / "data" / "foo", "rb") as stream:
        fs.upload_fobj(stream, to_info, 1)

    assert to_info.read_text() == "foo"


KB = 1024
MB = KB ** 2
GB = KB ** 3


def test_s3_aws_config(tmp_dir, dvc, s3, monkeypatch):
    config_directory = tmp_dir / ".aws"
    config_directory.mkdir()
    (config_directory / "config").write_text(
        textwrap.dedent(
            """\
    [default]
    s3 =
      max_concurrent_requests = 20000
      max_queue_size = 1000
      multipart_threshold = 1000KiB
      multipart_chunksize = 64MB
      use_accelerate_endpoint = true
      addressing_style = path
    """
        )
    )

    if sys.platform == "win32":
        var = "USERPROFILE"
    else:
        var = "HOME"
    monkeypatch.setenv(var, str(tmp_dir))

    # Fresh import to see the effects of changing HOME variable
    s3_mod = importlib.reload(sys.modules[S3FileSystem.__module__])
    fs = s3_mod.S3FileSystem(dvc, s3.config)
    assert fs._transfer_config is None

    with fs._get_s3() as s3:
        s3_config = s3.meta.client.meta.config.s3
        assert s3_config["use_accelerate_endpoint"]
        assert s3_config["addressing_style"] == "path"

    transfer_config = fs._transfer_config
    assert transfer_config.max_io_queue_size == 1000
    assert transfer_config.multipart_chunksize == 64 * MB
    assert transfer_config.multipart_threshold == 1000 * KB
    assert transfer_config.max_request_concurrency == 20000


def test_s3_aws_config_different_profile(tmp_dir, dvc, s3, monkeypatch):
    config_file = tmp_dir / "aws_config.ini"
    config_file.write_text(
        textwrap.dedent(
            """\
    [default]
    extra = keys
    s3 =
      addressing_style = auto
      use_accelerate_endpoint = true
      multipart_threshold = ThisIsNotGoingToBeCasted!
    [profile dev]
    some_extra = keys
    s3 =
      addresing_style = virtual
      multipart_threshold = 2GiB
    """
        )
    )
    monkeypatch.setenv("AWS_CONFIG_FILE", config_file)

    fs = S3FileSystem(dvc, {**s3.config, "profile": "dev"})
    assert fs._transfer_config is None

    with fs._get_s3() as s3:
        s3_config = s3.meta.client.meta.config.s3
        assert s3_config["addresing_style"] == "virtual"
        assert "use_accelerate_endpoint" not in s3_config

    transfer_config = fs._transfer_config
    assert transfer_config.multipart_threshold == 2 * GB
