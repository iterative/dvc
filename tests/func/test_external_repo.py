import os

from mock import patch

from dvc.external_repo import external_repo
from dvc.scm.git import Git
from dvc.compat import fspath
from dvc.utils.fs import path_isin


def test_external_repo(erepo_dir):
    with erepo_dir.chdir():
        with erepo_dir.branch("branch", new=True):
            erepo_dir.dvc_gen("file", "branch", commit="create file on branch")
        erepo_dir.dvc_gen("file", "master", commit="create file on master")

    url = fspath(erepo_dir)
    # We will share cache dir, to fetch version file
    cache_dir = erepo_dir.dvc.cache.local.cache_dir

    with patch.object(Git, "clone", wraps=Git.clone) as mock:
        with external_repo(url, cache_dir=cache_dir) as repo:
            with repo.open(os.path.join(repo.root_dir, "file")) as fd:
                assert fd.read() == "master"

        with external_repo(url, rev="branch", cache_dir=cache_dir) as repo:
            with repo.open(os.path.join(repo.root_dir, "file")) as fd:
                assert fd.read() == "branch"

        # Check cache_dir is unset
        with external_repo(url) as repo:
            assert path_isin(repo.cache.local.cache_dir, repo.root_dir)

        assert mock.call_count == 1
