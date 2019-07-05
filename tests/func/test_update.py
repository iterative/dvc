import os

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

    repo.scm.git.close()

    os.chdir(saved_dir)

    assert dvc_repo.status(stage.path) == {}
    dvc_repo.update(stage.path)
    assert dvc_repo.status(stage.path) == {}

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    with open(dst, "r+") as fobj:
        assert fobj.read() == "updated"
