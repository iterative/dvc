import os

import pytest

from dvc import api
from dvc.api import UrlNotDvcRepoError
from dvc.exceptions import FileMissingError
from dvc.main import main
from dvc.path_info import URLInfo
from dvc.utils.fs import remove
from tests.remotes import GCP, HDFS, OSS, S3, SSH, Azure, Local

remote_params = [S3, GCP, Azure, OSS, SSH, HDFS]
all_remote_params = [Local] + remote_params


@pytest.fixture
def remote_url(request):
    if not request.param.should_test():
        raise pytest.skip()
    return request.param.get_url()


def run_dvc(*argv):
    assert main(argv) == 0


@pytest.mark.parametrize("remote_url", remote_params, indirect=True)
def test_get_url(tmp_dir, dvc, remote_url):
    run_dvc("remote", "add", "-d", "upstream", remote_url)
    tmp_dir.dvc_gen("foo", "foo")

    expected_url = URLInfo(remote_url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url("foo") == expected_url


@pytest.mark.parametrize("remote_url", remote_params, indirect=True)
def test_get_url_external(erepo_dir, remote_url, setup_remote):
    setup_remote(erepo_dir.dvc, url=remote_url)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo", commit="add foo")

    # Using file url to force clone to tmp repo
    repo_url = f"file://{erepo_dir}"
    expected_url = URLInfo(remote_url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url("foo", repo=repo_url) == expected_url


def test_get_url_requires_dvc(tmp_dir, scm):
    tmp_dir.scm_gen({"foo": "foo"}, commit="initial")

    with pytest.raises(UrlNotDvcRepoError, match="not a DVC repository"):
        api.get_url("foo", repo=os.fspath(tmp_dir))

    with pytest.raises(UrlNotDvcRepoError):
        api.get_url("foo", repo=f"file://{tmp_dir}")


@pytest.mark.parametrize("remote_url", all_remote_params, indirect=True)
def test_open(remote_url, tmp_dir, dvc):
    run_dvc("remote", "add", "-d", "upstream", remote_url)
    tmp_dir.dvc_gen("foo", "foo-text")
    run_dvc("push")

    # Remove cache to force download
    remove(dvc.cache.local.cache_dir)

    with api.open("foo") as fd:
        assert fd.read() == "foo-text"


@pytest.mark.parametrize("remote_url", all_remote_params, indirect=True)
def test_open_external(remote_url, erepo_dir, setup_remote):
    setup_remote(erepo_dir.dvc, url=remote_url)

    with erepo_dir.chdir():
        erepo_dir.dvc_gen("version", "master", commit="add version")

        with erepo_dir.branch("branch", new="True"):
            # NOTE: need file to be other size for Mac
            erepo_dir.dvc_gen("version", "branchver", commit="add version")

    erepo_dir.dvc.push(all_branches=True)

    # Remove cache to force download
    remove(erepo_dir.dvc.cache.local.cache_dir)

    # Using file url to force clone to tmp repo
    repo_url = f"file://{erepo_dir}"
    with api.open("version", repo=repo_url) as fd:
        assert fd.read() == "master"

    assert api.read("version", repo=repo_url, rev="branch") == "branchver"


@pytest.mark.parametrize("remote_url", all_remote_params, indirect=True)
def test_missing(remote_url, tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    run_dvc("remote", "add", "-d", "upstream", remote_url)

    # Remove cache to make foo missing
    remove(dvc.cache.local.cache_dir)

    with pytest.raises(FileMissingError):
        api.read("foo")


def test_open_scm_controlled(tmp_dir, erepo_dir):
    erepo_dir.scm_gen({"scm_controlled": "file content"}, commit="create file")

    with api.open("scm_controlled", repo=os.fspath(erepo_dir)) as fd:
        assert fd.read() == "file content"


def test_open_not_cached(dvc):
    metric_file = "metric.txt"
    metric_content = "0.6"
    metric_code = "open('{}', 'w').write('{}')".format(
        metric_file, metric_content
    )
    dvc.run(
        single_stage=True,
        metrics_no_cache=[metric_file],
        cmd=(f'python -c "{metric_code}"'),
    )

    with api.open(metric_file) as fd:
        assert fd.read() == metric_content

    os.remove(metric_file)
    with pytest.raises(FileMissingError):
        api.read(metric_file)
