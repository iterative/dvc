from __future__ import unicode_literals

import os
import boto3
import filecmp

import pytest

from moto import mock_s3

from dvc.remote import RemoteS3
from dvc.repo import Repo
from dvc.utils import makedirs

from tests.func.test_data_cloud import get_aws_url


def test_get_file(repo_dir):
    src = repo_dir.FOO
    dst = repo_dir.FOO + "_imported"

    Repo.get_url(src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(repo_dir.FOO, dst, shallow=False)


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_get_url_to_dir(dname, repo_dir):
    src = repo_dir.DATA

    makedirs(dname, exist_ok=True)

    Repo.get_url(src, dname)

    dst = os.path.join(dname, os.path.basename(src))

    assert os.path.isdir(dname)
    assert filecmp.cmp(repo_dir.DATA, dst, shallow=False)


@mock_s3
@pytest.mark.parametrize("dst", [".", "./from"])
def test_get_url_from_non_local_path_to_dir_and_file(repo_dir, dst):
    file_name = "from"
    file_content = "data"
    base_info = RemoteS3.path_cls(get_aws_url())
    from_info = base_info / file_name

    s3 = boto3.client("s3")
    s3.create_bucket(Bucket=from_info.bucket)
    s3.put_object(
        Bucket=from_info.bucket, Key=from_info.path, Body=file_content
    )

    Repo.get_url(from_info.url, dst)

    result_path = os.path.join(dst, file_name) if os.path.isdir(dst) else dst

    assert os.path.exists(result_path)
    assert os.path.isfile(result_path)
    with open(result_path, "r") as fd:
        assert fd.read() == file_content
