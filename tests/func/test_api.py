import os

import pytest

from dvc import api
from dvc.api import UrlNotDvcRepoError
from dvc.exceptions import FileMissingError
from dvc.path_info import URLInfo
from dvc.utils.fs import remove
from tests.func.test_get import make_subrepo

cloud_names = [
    "s3",
    "gs",
    "azure",
    "gdrive",
    "oss",
    "ssh",
    "hdfs",
    "http",
]
clouds = [pytest.lazy_fixture(cloud) for cloud in cloud_names]
all_clouds = [pytest.lazy_fixture("local_cloud")] + clouds

# `lazy_fixture` is confusing pylint, pylint: disable=unused-argument


@pytest.mark.parametrize("remote", clouds, indirect=True)
def test_get_url(tmp_dir, dvc, remote):
    tmp_dir.dvc_gen("foo", "foo")

    expected_url = URLInfo(remote.url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url("foo") == expected_url


@pytest.mark.parametrize("cloud", clouds)
def test_get_url_external(erepo_dir, cloud):
    erepo_dir.add_remote(config=cloud.config)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo", commit="add foo")

    # Using file url to force clone to tmp repo
    repo_url = f"file://{erepo_dir}"
    expected_url = URLInfo(cloud.url) / "ac/bd18db4cc2f85cedef654fccc4a4d8"
    assert api.get_url("foo", repo=repo_url) == expected_url


def test_get_url_requires_dvc(tmp_dir, scm):
    tmp_dir.scm_gen({"foo": "foo"}, commit="initial")

    with pytest.raises(UrlNotDvcRepoError, match="not a DVC repository"):
        api.get_url("foo", repo=os.fspath(tmp_dir))

    with pytest.raises(UrlNotDvcRepoError):
        api.get_url("foo", repo=f"file://{tmp_dir}")


@pytest.mark.parametrize("remote", all_clouds, indirect=True)
def test_open(tmp_dir, dvc, remote):
    tmp_dir.dvc_gen("foo", "foo-text")
    dvc.push()

    # Remove cache to force download
    remove(dvc.cache.local.cache_dir)

    with api.open("foo") as fd:
        assert fd.read() == "foo-text"


@pytest.mark.parametrize(
    "cloud",
    [
        pytest.lazy_fixture(cloud)
        for cloud in [
            "real_s3",  # NOTE: moto's s3 fails in some tests
            "gs",
            "azure",
            "gdrive",
            "oss",
            "ssh",
            "hdfs",
            "http",
        ]
    ],
)
def test_open_external(erepo_dir, cloud):
    erepo_dir.add_remote(config=cloud.config)

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


@pytest.mark.parametrize("remote", all_clouds, indirect=True)
def test_open_granular(tmp_dir, dvc, remote):
    tmp_dir.dvc_gen({"dir": {"foo": "foo-text"}})
    dvc.push()

    # Remove cache to force download
    remove(dvc.cache.local.cache_dir)

    with api.open("dir/foo") as fd:
        assert fd.read() == "foo-text"


@pytest.mark.parametrize(
    "remote",
    [
        pytest.lazy_fixture(cloud)
        for cloud in [
            "real_s3",  # NOTE: moto's s3 fails in some tests
            "gs",
            "azure",
            "gdrive",
            "oss",
            "ssh",
            "hdfs",
            "http",
        ]
    ],
    indirect=True,
)
def test_missing(tmp_dir, dvc, remote):
    tmp_dir.dvc_gen("foo", "foo")

    # Remove cache to make foo missing
    remove(dvc.cache.local.cache_dir)

    api.read("foo")

    remove("foo")

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


def test_read_with_subrepos(tmp_dir, scm, local_cloud):
    # create a repo in "sub1", "sub2" and nested dvc repo in "sub2/nested"
    # each having a dvc-tracked file foo.txt with text of location:
    # eg: "sub1-foo.txt", etc. And "foo.txt" and "sub1/bar.txt" are git tracked
    tmp_dir.scm_gen("foo.txt", "foo.txt", commit="FOO")
    for path in ["sub1", "sub2", os.path.join("sub2", "nested")]:
        repo = tmp_dir / path
        make_subrepo(repo, scm, config=local_cloud.config)
        with repo.chdir():
            text = os.fspath((repo / "foo.txt").relative_to(tmp_dir)).replace(
                os.sep, "-"
            )
            repo.dvc_gen({"foo.txt": text}, commit=f"commit for path {path}")
            repo.dvc.push()

    tmp_dir.scm_gen(
        {"sub1": {"bar.txt": "sub1-bar.txt"}}, commit="add sub1/bar.txt"
    )
    repo_path = f"file://{tmp_dir}"  # forcing it to load as ExternalRepo

    assert api.read("foo.txt", repo=repo_path) == "foo.txt"
    assert api.read("sub1/foo.txt", repo=repo_path) == "sub1-foo.txt"
    assert api.read("sub1/bar.txt", repo=repo_path) == "sub1-bar.txt"
    assert api.read("sub2/foo.txt", repo=repo_path) == "sub2-foo.txt"

    nested_path = os.path.join("sub2", "nested", "foo.txt")
    assert api.read(nested_path, repo=repo_path) == "sub2-nested-foo.txt"


def test_get_url_subrepo_git_repo(tmp_dir, scm, local_cloud):
    remote_url = local_cloud.config["url"]
    checksums = []
    paths = ["sub1", "sub2", os.path.join("sub2", "nested")]
    for path in paths:
        repo = tmp_dir / path
        make_subrepo(repo, scm, config=local_cloud.config)
        with repo.chdir():
            text = os.fspath((repo / "foo.txt").relative_to(tmp_dir)).replace(
                "/", "-"
            )
            (stage,) = repo.dvc_gen(
                {"foo.txt": text}, commit=f"commit for path {path}"
            )
            repo.dvc.push()
        checksum = stage.outs[0].info["md5"]
        checksums.append(checksum)

    repo_path = f"file://{tmp_dir}"  # forcing it to load as ExternalRepo

    for path, c in zip(paths, checksums):
        url = api.get_url(os.path.join(path, "foo.txt"), repo=repo_path)
        assert url == os.path.join(os.path.relpath(remote_url), c[:2], c[2:])


def test_get_url_subrepo_dvc_on_toplevel(tmp_dir, dvc, scm, local_remote):
    remote_url = local_remote.config["url"]

    (stage,) = tmp_dir.dvc_gen("foo.txt", "foo.txt", commit="foo.txt")
    checksums = [stage.outs[0].checksum]
    tmp_dir.dvc.push()

    paths = ["sub1", "sub2", os.path.join("sub2", "nested")]
    for path in paths:
        repo = tmp_dir / path
        make_subrepo(repo, scm, config=local_remote.config)
        with repo.chdir():
            text = os.fspath((repo / "foo.txt").relative_to(tmp_dir)).replace(
                "/", "-"
            )
            (stage,) = repo.dvc_gen(
                {"foo.txt": text}, commit=f"commit for path {path}"
            )
            repo.dvc.push()
        checksum = stage.outs[0].checksum
        checksums.append(checksum)

    paths = [""] + paths
    repo_path = f"file://{tmp_dir}"  # forcing it to load as ExternalRepo

    for path, c in zip(paths, checksums):
        url = api.get_url(os.path.join(path, "foo.txt"), repo=repo_path)
        assert url == os.path.join(os.path.relpath(remote_url), c[:2], c[2:])
