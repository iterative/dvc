import os

from mock import ANY, patch

from dvc.external_repo import CLONES, external_repo
from dvc.path_info import PathInfo
from dvc.scm.git import Git
from dvc.tree.local import LocalTree
from dvc.utils import relpath
from dvc.utils.fs import remove


def test_external_repo(erepo_dir):
    with erepo_dir.chdir():
        with erepo_dir.branch("branch", new=True):
            erepo_dir.dvc_gen("file", "branch", commit="create file on branch")
        erepo_dir.dvc_gen("file", "master", commit="create file on master")

    url = os.fspath(erepo_dir)

    with patch.object(Git, "clone", wraps=Git.clone) as mock:
        with external_repo(url) as repo:
            with repo.open_by_relpath("file") as fd:
                assert fd.read() == "master"

        with external_repo(url, rev="branch") as repo:
            with repo.open_by_relpath("file") as fd:
                assert fd.read() == "branch"

        assert mock.call_count == 1


def test_source_change(erepo_dir):
    url = os.fspath(erepo_dir)
    with external_repo(url) as repo:
        old_rev = repo.scm.get_rev()

    erepo_dir.scm_gen("file", "text", commit="a change")

    with external_repo(url) as repo:
        new_rev = repo.scm.get_rev()

    assert old_rev != new_rev


def test_cache_reused(erepo_dir, mocker, local_cloud):
    erepo_dir.add_remote(config=local_cloud.config)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "text", commit="add file")
    erepo_dir.dvc.push()

    download_spy = mocker.spy(LocalTree, "download")

    # Use URL to prevent any fishy optimizations
    url = f"file://{erepo_dir}"
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

    url = f"file://{erepo_dir}"
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
        _, _, save_infos = repo.fetch_external(
            [os.path.join("subdir", "file")]
        )
        repo.cache.local.checkout(PathInfo(dest), save_infos[0])

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
    remove(erepo_dir.dvc.cache.local.cache_dir)

    url = os.fspath(erepo_dir)

    with external_repo(url) as repo:
        assert os.path.isabs(repo.config["remote"]["upstream"]["url"])
        assert os.path.isdir(repo.config["remote"]["upstream"]["url"])
        with repo.open_by_relpath("file") as fd:
            assert fd.read() == "contents"


def test_shallow_clone_branch(erepo_dir):
    with erepo_dir.chdir():
        with erepo_dir.branch("branch", new=True):
            erepo_dir.dvc_gen("file", "branch", commit="create file on branch")
        erepo_dir.dvc_gen("file", "master", commit="create file on master")

    url = os.fspath(erepo_dir)

    with patch.object(Git, "clone", wraps=Git.clone) as mock_clone:
        with external_repo(url, rev="branch") as repo:
            with repo.open_by_relpath("file") as fd:
                assert fd.read() == "branch"

        mock_clone.assert_called_with(url, ANY, shallow_branch="branch")
        _, shallow = CLONES[url]
        assert shallow

        with external_repo(url) as repo:
            with repo.open_by_relpath("file") as fd:
                assert fd.read() == "master"

        assert mock_clone.call_count == 1
        _, shallow = CLONES[url]
        assert not shallow


def test_shallow_clone_tag(erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "foo", commit="init")
        erepo_dir.scm.tag("v1")
        erepo_dir.dvc_gen("file", "bar", commit="update file")

    url = os.fspath(erepo_dir)

    with patch.object(Git, "clone", wraps=Git.clone) as mock_clone:
        with external_repo(url, rev="v1") as repo:
            with repo.open_by_relpath("file") as fd:
                assert fd.read() == "foo"

        mock_clone.assert_called_with(url, ANY, shallow_branch="v1")
        _, shallow = CLONES[url]
        assert shallow

        with external_repo(url, rev="master") as repo:
            with repo.open_by_relpath("file") as fd:
                assert fd.read() == "bar"

        assert mock_clone.call_count == 1
        _, shallow = CLONES[url]
        assert not shallow
