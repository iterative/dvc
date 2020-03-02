import os
import shutil

from mock import patch
from dvc.compat import fspath

from dvc.external_repo import external_repo
from dvc.scm.git import Git
from dvc.remote import RemoteLOCAL
from dvc.utils import relpath


def test_external_repo(erepo_dir):
    with erepo_dir.chdir():
        with erepo_dir.branch("branch", new=True):
            erepo_dir.dvc_gen("file", "branch", commit="create file on branch")
        erepo_dir.dvc_gen("file", "master", commit="create file on master")

    url = fspath(erepo_dir)

    with patch.object(Git, "clone", wraps=Git.clone) as mock:
        with external_repo(url) as repo:
            with repo.open_by_relpath("file") as fd:
                assert fd.read() == "master"

        with external_repo(url, rev="branch") as repo:
            with repo.open_by_relpath("file") as fd:
                assert fd.read() == "branch"

        assert mock.call_count == 1


def test_source_change(erepo_dir):
    url = fspath(erepo_dir)
    with external_repo(url) as repo:
        old_rev = repo.scm.get_rev()

    erepo_dir.scm_gen("file", "text", commit="a change")

    with external_repo(url) as repo:
        new_rev = repo.scm.get_rev()

    assert old_rev != new_rev


def test_cache_reused(erepo_dir, mocker):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "text", commit="add file")

    download_spy = mocker.spy(RemoteLOCAL, "download")

    # Use URL to prevent any fishy optimizations
    url = "file://{}".format(erepo_dir)
    with external_repo(url) as repo:
        repo.fetch()
        assert download_spy.mock.call_count == 1

    # Should not download second time
    erepo_dir.scm.branch("branch")
    with external_repo(url, "branch") as repo:
        repo.fetch()
        assert download_spy.mock.call_count == 1


def test_known_sha(erepo_dir):
    url = "file://{}".format(erepo_dir)
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
    with external_repo(fspath(erepo_dir)) as repo:
        repo.pull_to(os.path.join("subdir", "file"), dest)

    assert dest.is_file()
    assert dest.read_text() == "contents"


def test_relative_remote(erepo_dir, tmp_dir):
    # these steps reproduce the script on this issue:
    # https://github.com/iterative/dvc/issues/2756
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "contents", commit="create file")

    upstream_dir = tmp_dir
    upstream_url = relpath(upstream_dir, erepo_dir)
    with erepo_dir.dvc.config.edit() as conf:
        conf["remote"]["upstream"] = {"url": upstream_url}
        conf["core"]["remote"] = "upstream"

    erepo_dir.scm_add(
        erepo_dir.dvc.config.files["repo"], commit="Update dvc config"
    )
    erepo_dir.dvc.push()

    (erepo_dir / "file").unlink()
    shutil.rmtree(erepo_dir.dvc.cache.local.cache_dir)

    url = fspath(erepo_dir)

    with external_repo(url) as repo:
        assert os.path.isabs(repo.config["remote"]["upstream"]["url"])
        assert os.path.isdir(repo.config["remote"]["upstream"]["url"])
        with repo.open_by_relpath("file") as fd:
            assert fd.read() == "contents"
