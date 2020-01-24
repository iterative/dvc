from mock import patch
from dvc.compat import fspath

from dvc.external_repo import external_repo
from dvc.scm.git import Git
from dvc.remote import RemoteLOCAL


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
