from __future__ import unicode_literals

import os
import shutil

import pytest

from dvc import api
from dvc.exceptions import FileMissingError
from dvc.main import main
from dvc.path_info import URLInfo
from dvc.remote.config import RemoteConfig
from tests.remotes import Azure, GCP, HDFS, Local, OSS, S3, SSH


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
def test_get_url(remote_url, tmp_dir, dvc, repo_template):
    run_dvc("remote", "add", "-d", "upstream", remote_url)
    dvc.add("foo")

    expected_url = URLInfo(remote_url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url("foo") == expected_url


@pytest.mark.parametrize("remote_url", remote_params, indirect=True)
def test_get_url_external(remote_url, erepo_dir):
    _set_remote_url_and_commit(erepo_dir.dvc, remote_url)

    # Using file url to force clone to tmp repo
    repo_url = "file://{}".format(erepo_dir)
    expected_url = URLInfo(remote_url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url("foo", repo=repo_url) == expected_url


@pytest.mark.parametrize("remote_url", all_remote_params, indirect=True)
def test_open(remote_url, tmp_dir, dvc):
    run_dvc("remote", "add", "-d", "upstream", remote_url)
    tmp_dir.dvc_gen("foo", "foo-text")
    run_dvc("push")

    # Remove cache to force download
    shutil.rmtree(dvc.cache.local.cache_dir)

    with api.open("foo") as fd:
        assert fd.read() == "foo-text"


@pytest.mark.parametrize("remote_url", all_remote_params, indirect=True)
def test_open_external(remote_url, erepo_dir):
    erepo_dir.scm.checkout("branch")
    _set_remote_url_and_commit(erepo_dir.dvc, remote_url)
    erepo_dir.scm.checkout("master")
    _set_remote_url_and_commit(erepo_dir.dvc, remote_url)

    erepo_dir.dvc.push(all_branches=True)

    # Remove cache to force download
    shutil.rmtree(erepo_dir.dvc.cache.local.cache_dir)

    # Using file url to force clone to tmp repo
    repo_url = "file://{}".format(erepo_dir)
    with api.open("version", repo=repo_url) as fd:
        assert fd.read() == "master"

    assert api.read("version", repo=repo_url, rev="branch") == "branch"


@pytest.mark.parametrize("remote_url", all_remote_params, indirect=True)
def test_missing(remote_url, tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    run_dvc("remote", "add", "-d", "upstream", remote_url)

    # Remove cache to make foo missing
    shutil.rmtree(dvc.cache.local.cache_dir)

    with pytest.raises(FileMissingError):
        api.read("foo")


def _set_remote_url_and_commit(repo, remote_url):
    rconfig = RemoteConfig(repo.config)
    rconfig.modify("upstream", "url", remote_url)
    repo.scm.add([repo.config.config_file])
    repo.scm.commit("modify remote")


# FIXME: this test doesn't use scm ;D
def test_open_scm_controlled(dvc_repo, repo_dir):
    stage, = dvc_repo.add(repo_dir.FOO)

    stage_content = open(stage.path, "r").read()
    with api.open(stage.path) as fd:
        assert fd.read() == stage_content


# TODO: simplify, we shouldn't need run.
def test_open_not_cached(dvc):
    metric_file = "metric.txt"
    metric_content = "0.6"
    metric_code = "open('{}', 'w').write('{}')".format(
        metric_file, metric_content
    )
    dvc.run(
        metrics_no_cache=[metric_file],
        cmd=('python -c "{}"'.format(metric_code)),
    )

    with api.open(metric_file) as fd:
        assert fd.read() == metric_content

    os.remove(metric_file)
    with pytest.raises(FileMissingError):
        api.read(metric_file)
