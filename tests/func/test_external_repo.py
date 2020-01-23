from mock import patch
from dvc.compat import fspath

from dvc.external_repo import external_repo
from dvc.scm.git import Git


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
