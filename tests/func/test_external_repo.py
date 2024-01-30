import os

from dvc.repo.open_repo import CLONES
from dvc.repo.open_repo import _external_repo as external_repo
from dvc.scm import Git
from dvc.testing.tmp_dir import make_subrepo
from dvc.utils import relpath
from dvc.utils.fs import remove
from dvc_data.hashfile.build import build
from dvc_data.hashfile.transfer import transfer


def test_external_repo(erepo_dir, mocker):
    with erepo_dir.chdir():
        with erepo_dir.branch("branch", new=True):
            erepo_dir.dvc_gen("file", "branch", commit="create file on branch")
        erepo_dir.dvc_gen("file", "master", commit="create file on master")

    url = os.fspath(erepo_dir)

    clone_spy = mocker.spy(Git, "clone")

    with external_repo(url) as repo:
        with repo.dvcfs.open("file") as fd:
            assert fd.read() == "master"

    with external_repo(url, rev="branch") as repo:
        with repo.dvcfs.open("file") as fd:
            assert fd.read() == "branch"

    assert clone_spy.call_count == 1


def test_source_change(erepo_dir):
    url = os.fspath(erepo_dir)
    with external_repo(url) as repo:
        old_rev = repo.scm.get_rev()

    erepo_dir.scm_gen("file", "text", commit="a change")

    with external_repo(url) as repo:
        new_rev = repo.scm.get_rev()

    assert old_rev != new_rev


def test_cache_reused(erepo_dir, mocker, local_cloud):
    from dvc_objects.fs import generic

    erepo_dir.add_remote(config=local_cloud.config)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "text", commit="add file")
    erepo_dir.dvc.push()

    download_spy = mocker.spy(generic, "transfer")

    # Use URL to prevent any fishy optimizations
    url = f"file://{erepo_dir.as_posix()}"
    with external_repo(url) as repo:
        repo.fetch()
        assert download_spy.mock.call_count == 1

    # Should not download second time
    erepo_dir.scm.branch("branch")
    with external_repo(url, "branch") as repo:
        repo.fetch()
        assert download_spy.mock.call_count == 1


def test_known_sha(erepo_dir):
    erepo_dir.scm.commit("init")

    url = f"file://{erepo_dir.as_posix()}"
    with external_repo(url) as repo:
        rev = repo.scm.get_rev()
        prev_rev = repo.scm.resolve_rev("HEAD^")

    # Hits cache
    with external_repo(url, rev) as repo:
        pass

    # No clone, no pull, copies a repo, checks out the known sha
    with external_repo(url, prev_rev) as repo:
        pass


def test_pull_subdir_file(tmp_dir, erepo_dir):
    with erepo_dir.chdir():
        subdir = erepo_dir / "subdir"
        subdir.mkdir()
        (subdir / "file").write_text("contents")
        erepo_dir.dvc_add(subdir / "file", commit="create file")

    dest = tmp_dir / "file"
    with external_repo(os.fspath(erepo_dir)) as repo:
        repo.dvcfs.get("subdir/file", os.fspath(dest))

    assert dest.is_file()
    assert dest.read_text() == "contents"


def test_relative_remote(erepo_dir, tmp_dir):
    # these steps reproduce the script on this issue:
    # https://github.com/iterative/dvc/issues/2756
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "contents", commit="create file")

    upstream_dir = tmp_dir
    upstream_url = relpath(upstream_dir, erepo_dir)
    erepo_dir.add_remote(url=upstream_url)

    erepo_dir.dvc.push()

    (erepo_dir / "file").unlink()
    remove(erepo_dir.dvc.cache.local.path)

    url = os.fspath(erepo_dir)

    with external_repo(url) as repo:
        assert os.path.isabs(repo.config["remote"]["upstream"]["url"])
        assert os.path.isdir(repo.config["remote"]["upstream"]["url"])
        with repo.dvcfs.open("file") as fd:
            assert fd.read() == "contents"


def test_shallow_clone_branch(erepo_dir, mocker):
    with erepo_dir.chdir():
        with erepo_dir.branch("branch", new=True):
            erepo_dir.dvc_gen("file", "branch", commit="create file on branch")
        erepo_dir.dvc_gen("file", "master", commit="create file on master")

    url = os.fspath(erepo_dir)
    clone_spy = mocker.spy(Git, "clone")

    with external_repo(url, rev="branch") as repo:
        with repo.dvcfs.open("file") as fd:
            assert fd.read() == "branch"

    clone_spy.assert_called_with(
        url, mocker.ANY, shallow_branch="branch", progress=mocker.ANY
    )

    path, _ = CLONES[url]
    CLONES[url] = (path, True)

    mock_fetch = mocker.patch.object(Git, "fetch")
    with external_repo(url) as repo:
        with repo.dvcfs.open("file") as fd:
            assert fd.read() == "master"
    mock_fetch.assert_called_with(unshallow=True)


def test_shallow_clone_tag(erepo_dir, mocker):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "foo", commit="init")
        erepo_dir.scm.tag("v1")
        erepo_dir.dvc_gen("file", "bar", commit="update file")

    url = os.fspath(erepo_dir)

    clone_spy = mocker.spy(Git, "clone")
    with external_repo(url, rev="v1") as repo:
        with repo.dvcfs.open("file") as fd:
            assert fd.read() == "foo"

    clone_spy.assert_called_with(
        url, mocker.ANY, shallow_branch="v1", progress=mocker.ANY
    )

    path, _ = CLONES[url]
    CLONES[url] = (path, True)

    mock_fetch = mocker.patch.object(Git, "fetch")
    with external_repo(url, rev="master") as repo:
        with repo.dvcfs.open("file") as fd:
            assert fd.read() == "bar"
    mock_fetch.assert_called_with(unshallow=True)


def test_subrepos_are_ignored(tmp_dir, erepo_dir):
    subrepo = erepo_dir / "dir" / "subrepo"
    make_subrepo(subrepo, erepo_dir.scm)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("dir/foo", "foo", commit="foo")
        erepo_dir.scm_gen("dir/bar", "bar", commit="bar")

    with subrepo.chdir():
        subrepo.dvc_gen({"file": "file"}, commit="add files on subrepo")

    with external_repo(os.fspath(erepo_dir)) as repo:
        repo.dvcfs.get("dir", os.fspath(tmp_dir / "out"))
        expected_files = {"foo": "foo", "bar": "bar", ".gitignore": "/foo\n"}
        assert (tmp_dir / "out").read_text() == expected_files

        # clear cache to test saving to cache
        cache_dir = tmp_dir / repo.cache.local.path
        remove(cache_dir)
        os.makedirs(cache_dir)

        staging, _, obj = build(
            repo.cache.local,
            "dir",
            repo.dvcfs,
            "md5",
            ignore=repo.dvcignore,
        )
        transfer(
            staging,
            repo.cache.local,
            {obj.hash_info},
            shallow=False,
            hardlink=True,
        )
        if os.name == "nt":
            expected_gitignore_path = (
                cache_dir / "d5" / "61e684092f0ff908aa82ee9cc1e594"
            )
            expected_dir_path = cache_dir / "0d" / "2086760aea091f1504eafc8843bb18.dir"
        else:
            expected_gitignore_path = (
                cache_dir / "94" / "7d2b84e5aa88170e80dff467a5bfb6"
            )
            expected_dir_path = cache_dir / "e1" / "d9e8eae5374860ae025ec84cfd85c7.dir"
        assert set(cache_dir.glob("??/*")) == {
            expected_dir_path,
            expected_gitignore_path,
            cache_dir / "37" / "b51d194a7513e45b56f6524f2d51f2",
            cache_dir / "ac" / "bd18db4cc2f85cedef654fccc4a4d8",
        }


def test_subrepos_are_ignored_for_git_tracked_dirs(tmp_dir, erepo_dir):
    subrepo = erepo_dir / "dir" / "subrepo"
    make_subrepo(subrepo, erepo_dir.scm)
    with erepo_dir.chdir():
        scm_files = {"foo": "foo", "bar": "bar", "subdir": {"lorem": "lorem"}}
        erepo_dir.scm_gen({"dir": scm_files}, commit="add scm dir")

    with subrepo.chdir():
        subrepo.dvc_gen({"file": "file"}, commit="add files on subrepo")

    with external_repo(os.fspath(erepo_dir)) as repo:
        repo.dvcfs.get("dir", os.fspath(tmp_dir / "out"))
        # subrepo files should not be here
        assert (tmp_dir / "out").read_text() == scm_files
