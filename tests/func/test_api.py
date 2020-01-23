import os
import shutil
import copy

import ruamel.yaml
import pytest

from dvc import api
from dvc.api import SummonError, UrlNotDvcRepoError
from dvc.compat import fspath
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
def test_get_url(tmp_dir, dvc, remote_url):
    run_dvc("remote", "add", "-d", "upstream", remote_url)
    tmp_dir.dvc_gen("foo", "foo")

    expected_url = URLInfo(remote_url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url("foo") == expected_url


@pytest.mark.parametrize("remote_url", remote_params, indirect=True)
def test_get_url_external(erepo_dir, remote_url):
    _set_remote_url_and_commit(erepo_dir.dvc, remote_url)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo", commit="add foo")

    # Using file url to force clone to tmp repo
    repo_url = "file://{}".format(erepo_dir)
    expected_url = URLInfo(remote_url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url("foo", repo=repo_url) == expected_url


def test_get_url_requires_dvc(tmp_dir, scm):
    tmp_dir.scm_gen({"foo": "foo"}, commit="initial")

    with pytest.raises(UrlNotDvcRepoError, match="not a DVC repository"):
        api.get_url("foo", repo=fspath(tmp_dir))

    with pytest.raises(UrlNotDvcRepoError):
        api.get_url("foo", repo="file://{}".format(tmp_dir))


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
    _set_remote_url_and_commit(erepo_dir.dvc, remote_url)

    with erepo_dir.chdir():
        erepo_dir.dvc_gen("version", "master", commit="add version")

        with erepo_dir.branch("branch", new="True"):
            # NOTE: need file to be other size for Mac
            erepo_dir.dvc_gen("version", "branchver", commit="add version")

    erepo_dir.dvc.push(all_branches=True)

    # Remove cache to force download
    shutil.rmtree(erepo_dir.dvc.cache.local.cache_dir)

    # Using file url to force clone to tmp repo
    repo_url = "file://{}".format(erepo_dir)
    with api.open("version", repo=repo_url) as fd:
        assert fd.read() == "master"

    assert api.read("version", repo=repo_url, rev="branch") == "branchver"


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


def test_open_scm_controlled(tmp_dir, erepo_dir):
    erepo_dir.scm_gen({"scm_controlled": "file content"}, commit="create file")

    with api.open("scm_controlled", repo=fspath(erepo_dir)) as fd:
        assert fd.read() == "file content"


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


def test_summon(tmp_dir, dvc, erepo_dir):
    objects = {
        "objects": [
            {
                "name": "sum",
                "meta": {"description": "Add <x> to <number>"},
                "summon": {
                    "type": "python",
                    "call": "calculator.add_to_num",
                    "args": {"x": 1},
                    "deps": ["number"],
                },
            }
        ]
    }

    other_objects = copy.deepcopy(objects)
    other_objects["objects"][0]["summon"]["args"]["x"] = 100

    dup_objects = copy.deepcopy(objects)
    dup_objects["objects"] *= 2

    with erepo_dir.chdir():
        erepo_dir.dvc_gen("number", "100", commit="Add number.dvc")
        erepo_dir.scm_gen("dvcsummon.yaml", ruamel.yaml.dump(objects))
        erepo_dir.scm_gen("other.yaml", ruamel.yaml.dump(other_objects))
        erepo_dir.scm_gen("dup.yaml", ruamel.yaml.dump(dup_objects))
        erepo_dir.scm_gen("invalid.yaml", ruamel.yaml.dump({"name": "sum"}))
        erepo_dir.scm_gen("not_yaml.yaml", "a: - this is not a YAML file")
        erepo_dir.scm_gen(
            "calculator.py",
            "def add_to_num(x): return x + int(open('number').read())",
        )
        erepo_dir.scm.commit("Add files")

    repo_url = "file://{}".format(erepo_dir)

    assert api.summon("sum", repo=repo_url) == 101
    assert api.summon("sum", repo=repo_url, args={"x": 2}) == 102
    assert api.summon("sum", repo=repo_url, summon_file="other.yaml") == 200

    try:
        api.summon("sum", repo=repo_url, summon_file="missing.yaml")
    except SummonError as exc:
        assert "Summon file not found" in str(exc)
        assert "missing.yaml" in str(exc)
        assert repo_url in str(exc)
    else:
        pytest.fail("Did not raise on missing summon file")

    with pytest.raises(SummonError, match=r"No object with name 'missing'"):
        api.summon("missing", repo=repo_url)

    with pytest.raises(
        SummonError, match=r"More than one object with name 'sum'"
    ):
        api.summon("sum", repo=repo_url, summon_file="dup.yaml")

    with pytest.raises(SummonError, match=r"extra keys not allowed"):
        api.summon("sum", repo=repo_url, summon_file="invalid.yaml")

    with pytest.raises(SummonError, match=r"Failed to parse summon file"):
        api.summon("sum", repo=repo_url, summon_file="not_yaml.yaml")
