import os

import pytest
from funcy import first, get_in

from dvc import api
from dvc.exceptions import OutputNotFoundError, PathMissingError
from dvc.scm import CloneError
from dvc.testing.api_tests import TestAPI  # noqa: F401
from dvc.testing.tmp_dir import make_subrepo
from dvc.utils.fs import remove


def test_get_url_external(tmp_dir, erepo_dir, cloud):
    erepo_dir.add_remote(config=cloud.config)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo", commit="add foo")

    # Using file url to force clone to tmp repo
    repo_url = f"file://{erepo_dir.as_posix()}"
    expected_url = (cloud / "files" / "md5" / "ac/bd18db4cc2f85cedef654fccc4a4d8").url
    assert api.get_url("foo", repo=repo_url) == expected_url


def test_get_url_requires_dvc(tmp_dir, scm):
    tmp_dir.scm_gen({"foo": "foo"}, commit="initial")

    with pytest.raises(OutputNotFoundError, match="output 'foo'"):
        api.get_url("foo", repo=os.fspath(tmp_dir))

    with pytest.raises(OutputNotFoundError, match="output 'foo'"):
        api.get_url("foo", repo=f"file://{tmp_dir.as_posix()}")


def test_get_url_from_remote(tmp_dir, erepo_dir, cloud, local_cloud):
    erepo_dir.add_remote(config=cloud.config, name="other")
    erepo_dir.add_remote(config=local_cloud.config, default=True)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo", commit="add foo")

    # Using file url to force clone to tmp repo
    repo_url = f"file://{erepo_dir.as_posix()}"
    expected_rel_path = os.path.join(
        "files", "md5", "ac/bd18db4cc2f85cedef654fccc4a4d8"
    )

    # Test default remote
    assert api.get_url("foo", repo=repo_url) == (local_cloud / expected_rel_path).url

    # Test remote arg
    assert (
        api.get_url("foo", repo=repo_url, remote="other")
        == (cloud / expected_rel_path).url
    )

    # Test config arg
    assert (
        api.get_url("foo", repo=repo_url, config={"core": {"remote": "other"}})
        == (cloud / expected_rel_path).url
    )

    # Test remote_config arg
    assert (
        api.get_url("foo", repo=repo_url, remote_config={"url": cloud.url})
        == (cloud / expected_rel_path).url
    )


def test_get_url_ignore_scm(tmp_dir, dvc, cloud, scm):
    tmp_dir.add_remote(config=cloud.config)
    tmp_dir.dvc_gen("foo", "foo", commit="add foo")

    repo_posix = tmp_dir.as_posix()
    expected_url = (cloud / "files" / "md5" / "ac/bd18db4cc2f85cedef654fccc4a4d8").url

    # Test baseline with scm
    assert api.get_url("foo", repo=repo_posix) == expected_url

    # Simulate gitless environment (e.g. deployed container)
    (tmp_dir / ".git").rename(tmp_dir / "gitless_environment")

    assert api.get_url("foo", repo=repo_posix) == expected_url
    assert (
        api.get_url("foo", repo=repo_posix, config={"core": {"no_scm": True}})
        == expected_url
    )

    # Addressing repos with `file://` triggers git, so it fails in a gitless environment
    repo_url = f"file://{repo_posix}"
    with pytest.raises(
        CloneError,
        match="SCM error",
    ):
        api.get_url("foo", repo=repo_url, config={"core": {"no_scm": True}})


def test_open_external(tmp_dir, erepo_dir, cloud):
    erepo_dir.add_remote(config=cloud.config)

    with erepo_dir.chdir():
        erepo_dir.dvc_gen("version", "master", commit="add version")

        with erepo_dir.branch("branch", new="True"):
            # NOTE: need file to be other size for Mac
            erepo_dir.dvc_gen("version", "branchver", commit="add version")

    assert erepo_dir.dvc.push(all_branches=True) == 2

    # Remove cache to force download
    remove(erepo_dir.dvc.cache.local.path)

    # Using file url to force clone to tmp repo
    repo_url = f"file://{erepo_dir.as_posix()}"
    with api.open("version", repo=repo_url) as fd:
        assert fd.read() == "master"

    assert api.read("version", repo=repo_url, rev="branch") == "branchver"


def test_open_granular(tmp_dir, dvc, remote):
    tmp_dir.dvc_gen({"dir": {"foo": "foo-text"}})
    dvc.push()

    # Remove cache to force download
    remove(dvc.cache.local.path)

    with api.open("dir/foo") as fd:
        assert fd.read() == "foo-text"


def test_missing(tmp_dir, dvc, remote):
    tmp_dir.dvc_gen("foo", "foo")

    # Remove cache to make foo missing
    remove(dvc.cache.local.path)

    api.read("foo")

    remove("foo")

    with pytest.raises(PathMissingError):
        api.read("foo")


def test_open_scm_controlled(tmp_dir, erepo_dir):
    erepo_dir.scm_gen({"scm_controlled": "file content"}, commit="create file")

    with api.open("scm_controlled", repo=os.fspath(erepo_dir)) as fd:
        assert fd.read() == "file content"


def test_open_not_cached(dvc):
    metric_file = "metric.txt"
    metric_content = "0.6"
    metric_code = f"open('{metric_file}', 'w').write('{metric_content}')"
    dvc.run(
        name="write-metric",
        metrics_no_cache=[metric_file],
        cmd=f'python -c "{metric_code}"',
    )

    with api.open(metric_file) as fd:
        assert fd.read() == metric_content

    os.remove(metric_file)
    with pytest.raises(PathMissingError):
        api.read(metric_file)


def test_open_rev(tmp_dir, scm, dvc):
    tmp_dir.scm_gen("foo", "foo", commit="foo")

    (tmp_dir / "foo").write_text("bar")

    with api.open("foo", rev="master") as fobj:
        assert fobj.read() == "foo"


@pytest.mark.parametrize("as_external", [True, False])
@pytest.mark.parametrize(
    "files, to_read",
    [
        ({"foo": "foo"}, "foo"),
        ({"dir": {"foo": "foo", "bar": "bar"}}, os.path.join("dir", "foo")),
    ],
    ids=["file", "inside-dir"],
)
def test_api_missing_local_cache_exists_on_remote(
    tmp_dir, scm, dvc, as_external, remote, files, to_read
):
    tmp_dir.dvc_gen(files, commit="DVC track files")
    dvc.push()

    # Remove cache to make foo missing
    remove(dvc.cache.local.path)
    remove(first(files))

    repo_url = f"file://{tmp_dir.as_posix()}" if as_external else None
    file_content = get_in(files, to_read.split(os.sep))
    assert api.read(to_read, repo=repo_url) == file_content


@pytest.mark.parametrize("local_repo", [False, True])
def test_read_with_subrepos(tmp_dir, scm, local_cloud, local_repo):
    tmp_dir.scm_gen("foo.txt", "foo.txt", commit="add foo.txt")
    subrepo = tmp_dir / "dir" / "subrepo"
    make_subrepo(subrepo, scm, config=local_cloud.config)
    with subrepo.chdir():
        subrepo.scm_gen({"lorem": "lorem"}, commit="add lorem")
        subrepo.dvc_gen({"dir": {"file.txt": "file.txt"}}, commit="add dir")
        subrepo.dvc_gen("dvc-file", "dvc-file", commit="add dir")
        subrepo.dvc.push()

    repo_path = None if local_repo else f"file://{tmp_dir.as_posix()}"
    subrepo_path = os.path.join("dir", "subrepo")

    assert api.read("foo.txt", repo=repo_path) == "foo.txt"
    assert api.read(os.path.join(subrepo_path, "lorem"), repo=repo_path) == "lorem"
    assert (
        api.read(os.path.join(subrepo_path, "dvc-file"), repo=repo_path) == "dvc-file"
    )
    assert (
        api.read(os.path.join(subrepo_path, "dir", "file.txt"), repo=repo_path)
        == "file.txt"
    )


def test_get_url_granular(tmp_dir, dvc, cloud):
    tmp_dir.add_remote(config=cloud.config)
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar", "nested": {"file": "file"}}})

    expected_url = (
        cloud / "files" / "md5" / "5f" / "c28ea78987408341668eba6525ebd1.dir"
    ).url
    assert api.get_url("dir") == expected_url

    expected_url = (
        cloud / "files" / "md5" / "ac" / "bd18db4cc2f85cedef654fccc4a4d8"
    ).url
    assert api.get_url("dir/foo") == expected_url

    expected_url = (
        cloud / "files" / "md5" / "37" / "b51d194a7513e45b56f6524f2d51f2"
    ).url
    assert api.get_url("dir/bar") == expected_url

    expected_url = (
        cloud / "files" / "md5" / "8c" / "7dd922ad47494fc02c388e12c00eac"
    ).url
    assert api.get_url(os.path.join("dir", "nested", "file")) == expected_url


def test_get_url_subrepos(tmp_dir, scm, local_cloud):
    subrepo = tmp_dir / "subrepo"
    make_subrepo(subrepo, scm, config=local_cloud.config)
    with subrepo.chdir():
        subrepo.dvc_gen({"dir": {"foo": "foo"}, "bar": "bar"}, commit="add files")
        subrepo.dvc.push()

    expected_url = os.fspath(
        local_cloud / "files" / "md5" / "ac" / "bd18db4cc2f85cedef654fccc4a4d8"
    )
    assert api.get_url(os.path.join("subrepo", "dir", "foo")) == expected_url
    assert api.get_url(os.path.join("subrepo", "dir", "foo"), repo=".") == expected_url

    expected_url = os.fspath(
        local_cloud / "files" / "md5" / "37" / "b51d194a7513e45b56f6524f2d51f2"
    )
    assert api.get_url("subrepo/bar") == expected_url
    assert api.get_url("subrepo/bar", repo=".") == expected_url


def test_open_from_remote(tmp_dir, erepo_dir, cloud, local_cloud):
    erepo_dir.add_remote(config=cloud.config, name="other")
    erepo_dir.add_remote(config=local_cloud.config, default=True)
    erepo_dir.dvc_gen({"dir": {"foo": "foo content"}}, commit="create file")
    erepo_dir.dvc.push(remote="other")
    remove(erepo_dir.dvc.cache.local.path)

    with api.open(
        os.path.join("dir", "foo"),
        repo=f"file://{erepo_dir.as_posix()}",
        remote="other",
    ) as fd:
        assert fd.read() == "foo content"

    with api.open(
        os.path.join("dir", "foo"),
        repo=f"file://{erepo_dir.as_posix()}",
        config={"core": {"remote": "other"}},
    ) as fd:
        assert fd.read() == "foo content"


def test_read_from_remote(tmp_dir, erepo_dir, cloud, local_cloud):
    erepo_dir.add_remote(config=cloud.config, name="other")
    erepo_dir.add_remote(config=local_cloud.config, default=True)
    erepo_dir.dvc_gen({"dir": {"foo": "foo content"}}, commit="create file")
    erepo_dir.dvc.push(remote="other")
    remove(erepo_dir.dvc.cache.local.path)

    assert (
        api.read(
            os.path.join("dir", "foo"),
            repo=f"file://{erepo_dir.as_posix()}",
            remote="other",
        )
        == "foo content"
    )

    assert (
        api.read(
            os.path.join("dir", "foo"),
            repo=f"file://{erepo_dir.as_posix()}",
            config={"core": {"remote": "other"}},
        )
        == "foo content"
    )

    assert (
        api.read(
            os.path.join("dir", "foo"),
            repo=f"file://{erepo_dir.as_posix()}",
            remote_config={"url": cloud.url},
        )
        == "foo content"
    )
