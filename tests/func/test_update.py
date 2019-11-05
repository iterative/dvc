import filecmp
import os
import shutil

from dvc.external_repo import clean_repos
from dvc.repo import Repo


def test_update_import(dvc_repo, erepo):
    src = "version"
    dst = src

    stage = dvc_repo.imp(erepo.root_dir, src, dst, rev="branch")

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    with open(dst, "r+") as fobj:
        assert fobj.read() == "branch"

    # update data
    repo = Repo(erepo.root_dir)

    saved_dir = os.getcwd()
    os.chdir(erepo.root_dir)

    repo.scm.checkout("branch")
    os.unlink("version")
    erepo.create("version", "updated")
    repo.add("version")
    repo.scm.add([".gitignore", "version.dvc"])
    repo.scm.commit("updated")
    repo.scm.checkout("master")

    repo.scm.close()

    os.chdir(saved_dir)

    # Caching in external repos doesn't see upstream updates within single
    # cli call, so we need to clean the caches to see the changes.
    clean_repos()

    assert dvc_repo.status([stage.path]) == {}
    dvc_repo.update(stage.path)
    assert dvc_repo.status([stage.path]) == {}

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    with open(dst, "r+") as fobj:
        assert fobj.read() == "updated"


def test_update_import_url(repo_dir, dvc_repo):
    src = "file"
    dst = src + "_imported"

    shutil.copyfile(repo_dir.FOO, src)

    stage = dvc_repo.imp_url(src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(src, dst, shallow=False)

    # update data
    os.unlink(src)
    shutil.copyfile(repo_dir.BAR, src)

    assert dvc_repo.status([stage.path]) == {}
    dvc_repo.update(stage.path)
    assert dvc_repo.status([stage.path]) == {}

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(src, dst, shallow=False)
