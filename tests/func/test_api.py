import pytest
import shutil

from dvc import api
from dvc.main import main
from .test_data_cloud import (
    _should_test_aws,
    get_aws_url,
    _should_test_gcp,
    get_gcp_url,
    _should_test_azure,
    get_azure_url,
    _should_test_oss,
    get_oss_url,
    _should_test_ssh,
    get_ssh_url,
    _should_test_hdfs,
    get_hdfs_url,
)


# NOTE: staticmethod is only needed in Python 2
class S3:
    should_test = staticmethod(_should_test_aws)
    get_url = staticmethod(get_aws_url)


class GCP:
    should_test = staticmethod(_should_test_gcp)
    get_url = staticmethod(get_gcp_url)


class Azure:
    should_test = staticmethod(_should_test_azure)
    get_url = staticmethod(get_azure_url)


class OSS:
    should_test = staticmethod(_should_test_oss)
    get_url = staticmethod(get_oss_url)


class SSH:
    should_test = staticmethod(_should_test_ssh)
    get_url = staticmethod(get_ssh_url)


class HDFS:
    should_test = staticmethod(_should_test_hdfs)
    get_url = staticmethod(get_hdfs_url)


remote_params = [S3, GCP, Azure, OSS, SSH, HDFS]


@pytest.fixture
def remote(request):
    if not request.param.should_test():
        raise pytest.skip()
    return request.param


def pytest_generate_tests(metafunc):
    if "remote" in metafunc.fixturenames:
        metafunc.parametrize("remote", remote_params, indirect=True)


def run_dvc(*argv):
    assert main(argv) == 0


def test_get_url(repo_dir, dvc_repo, remote):
    remote_url = remote.get_url()

    run_dvc("remote", "add", "-d", "upstream", remote_url)
    dvc_repo.add(repo_dir.FOO)

    assert api.get_url(repo_dir.FOO) == "%s/%s" % (
        remote_url,
        "ac/bd18db4cc2f85cedef654fccc4a4d8",
    )


def test_open(repo_dir, dvc_repo, remote):
    run_dvc("remote", "add", "-d", "upstream", remote.get_url())
    dvc_repo.add(repo_dir.FOO)
    run_dvc("push")

    # Remove cache to force download
    shutil.rmtree(dvc_repo.cache.local.cache_dir)

    with api.open(repo_dir.FOO) as fd:
        assert fd.read() == repo_dir.FOO_CONTENTS
