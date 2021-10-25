import pytest

from dvc.fs.s3 import S3FileSystem
from dvc.objects.db import _get_odb

remotes = [pytest.lazy_fixture(fix) for fix in ["gs", "s3"]]

FILE_WITH_CONTENTS = {
    "data1.txt": "",
    #   "empty_dir/": "",
    "empty_file": "",
    "foo": "foo",
    "data/alice": "alice",
    "data/alpha": "alpha",
    "data/subdir-file.txt": "subdir",
    "data/subdir/1": "1",
    "data/subdir/2": "2",
    "data/subdir/3": "3",
    #   "data/subdir/empty_dir/": "",
    "data/subdir/empty_file": "",
}


@pytest.mark.parametrize("cloud", [pytest.lazy_fixture("s3")])
def test_copy_preserve_etag_across_buckets(cloud, dvc):
    cloud.gen(FILE_WITH_CONTENTS)
    rem = _get_odb(dvc, cloud.config)
    s3 = rem.fs.s3
    s3.create_bucket(Bucket="another")

    config = cloud.config.copy()
    config["url"] = "s3://another"
    config["region"] = "us-east-1"

    another = S3FileSystem(**config)

    from_info = rem.fs.path.join(rem.fs_path, "foo")
    to_info = "another/foo"

    rem.fs.copy(from_info, to_info)

    from_hash = rem.fs.info(from_info)["ETag"].strip('"')
    to_hash = another.info(to_info)["ETag"].strip('"')

    assert from_hash == to_hash
