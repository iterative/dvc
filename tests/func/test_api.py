import pytest
import shutil

from dvc import api
from dvc.exceptions import OutputFileMissingError
from dvc.main import main
from dvc.path_info import URLInfo
from dvc.remote.config import RemoteConfig
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
def remote_url(request):
    if not request.param.should_test():
        raise pytest.skip()
    return request.param.get_url()


def pytest_generate_tests(metafunc):
    if "remote_url" in metafunc.fixturenames:
        metafunc.parametrize("remote_url", remote_params, indirect=True)


def run_dvc(*argv):
    assert main(argv) == 0


def test_get_url(repo_dir, dvc_repo, remote_url):
    run_dvc("remote", "add", "-d", "upstream", remote_url)
    dvc_repo.add(repo_dir.FOO)

    expected_url = URLInfo(remote_url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url(repo_dir.FOO) == expected_url


def test_get_url_external(repo_dir, dvc_repo, erepo, remote_url):
    _set_remote_url_and_commit(erepo.dvc, remote_url)

    # Using file url to force clone to tmp repo
    repo_url = "file://" + erepo.dvc.root_dir
    expected_url = URLInfo(remote_url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url(repo_dir.FOO, repo=repo_url) == expected_url


def test_open(repo_dir, dvc_repo, remote_url):
    run_dvc("remote", "add", "-d", "upstream", remote_url)
    dvc_repo.add(repo_dir.FOO)
    run_dvc("push")

    # Remove cache to force download
    shutil.rmtree(dvc_repo.cache.local.cache_dir)

    with api.open(repo_dir.FOO) as fd:
        assert fd.read() == repo_dir.FOO_CONTENTS


def test_open_external(repo_dir, dvc_repo, erepo, remote_url):
    erepo.dvc.scm.checkout("branch")
    _set_remote_url_and_commit(erepo.dvc, remote_url)
    erepo.dvc.scm.checkout("master")
    _set_remote_url_and_commit(erepo.dvc, remote_url)

    erepo.dvc.push(all_branches=True)

    # Remove cache to force download
    shutil.rmtree(erepo.dvc.cache.local.cache_dir)

    # Using file url to force clone to tmp repo
    repo_url = "file://" + erepo.dvc.root_dir
    with api.open("version", repo=repo_url) as fd:
        assert fd.read() == "master"

    assert api.read("version", repo=repo_url, rev="branch") == "branch"


def test_open_missing(erepo):
    # Remove cache to make foo missing
    shutil.rmtree(erepo.dvc.cache.local.cache_dir)

    repo_url = "file://" + erepo.dvc.root_dir
    with pytest.raises(OutputFileMissingError):
        api.read(erepo.FOO, repo=repo_url)


def _set_remote_url_and_commit(repo, remote_url):
    rconfig = RemoteConfig(repo.config)
    rconfig.modify("upstream", "url", remote_url)
    repo.scm.add([repo.config.config_file])
    repo.scm.commit("modify remote")
