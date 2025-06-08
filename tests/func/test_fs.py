import os
from operator import itemgetter

from dvc.repo import Repo


def test_cleanfs_subrepo(tmp_dir, dvc, scm, monkeypatch):
    tmp_dir.gen({"subdir": {}})
    subrepo_dir = tmp_dir / "subdir"
    with subrepo_dir.chdir():
        subrepo = Repo.init(subdir=True)
        subrepo_dir.gen({"foo": "foo", "dir": {"bar": "bar"}})

    path = subrepo_dir.fs_path

    assert dvc.fs.exists(dvc.fs.join(path, "foo"))
    assert dvc.fs.isfile(dvc.fs.join(path, "foo"))
    assert dvc.fs.exists(dvc.fs.join(path, "dir"))
    assert dvc.fs.isdir(dvc.fs.join(path, "dir"))

    assert subrepo.fs.exists(subrepo.fs.join(path, "foo"))
    assert subrepo.fs.isfile(subrepo.fs.join(path, "foo"))
    assert subrepo.fs.exists(subrepo.fs.join(path, "dir"))
    assert subrepo.fs.isdir(subrepo.fs.join(path, "dir"))


def test_walk_dont_ignore_subrepos(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"foo": "foo"}, commit="add foo")
    subrepo_dir = tmp_dir / "subdir"
    subrepo_dir.mkdir()
    with subrepo_dir.chdir():
        Repo.init(subdir=True)
    scm.add(["subdir"])
    scm.commit("Add subrepo")

    dvc_fs = dvc.fs
    dvc._reset()
    scm_fs = scm.get_fs("HEAD")
    path = os.fspath(tmp_dir)
    get_dirs = itemgetter(1)

    assert set(get_dirs(next(dvc_fs.walk(path)))) == {".dvc", "subdir", ".git"}
    assert set(get_dirs(next(scm_fs.walk("/")))) == {".dvc", "subdir"}
