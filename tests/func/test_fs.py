import os
from operator import itemgetter

from dvc.fs.callbacks import Callback
from dvc.repo import Repo


def test_cleanfs_subrepo(tmp_dir, dvc, scm, monkeypatch):
    tmp_dir.gen({"subdir": {}})
    subrepo_dir = tmp_dir / "subdir"
    with subrepo_dir.chdir():
        subrepo = Repo.init(subdir=True)
        subrepo_dir.gen({"foo": "foo", "dir": {"bar": "bar"}})

    path = subrepo_dir.fs_path

    assert dvc.fs.exists(dvc.fs.path.join(path, "foo"))
    assert dvc.fs.isfile(dvc.fs.path.join(path, "foo"))
    assert dvc.fs.exists(dvc.fs.path.join(path, "dir"))
    assert dvc.fs.isdir(dvc.fs.path.join(path, "dir"))

    assert subrepo.fs.exists(subrepo.fs.path.join(path, "foo"))
    assert subrepo.fs.isfile(subrepo.fs.path.join(path, "foo"))
    assert subrepo.fs.exists(subrepo.fs.path.join(path, "dir"))
    assert subrepo.fs.isdir(subrepo.fs.path.join(path, "dir"))


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


def test_callback_on_dvcfs(tmp_dir, dvc, scm, mocker):
    tmp_dir.dvc_gen({"dir": {"bar": "bar"}}, commit="dvc")
    tmp_dir.scm_gen({"dir": {"foo": "foo"}}, commit="git")

    fs = dvc.dvcfs

    callback = Callback()
    fs.get(
        "dir",
        (tmp_dir / "dir2").fs_path,
        callback=callback,
    )

    assert (tmp_dir / "dir2").read_text() == {"foo": "foo", "bar": "bar"}
    assert callback.size == 2
    assert callback.value == 2

    callback = Callback()
    branch = mocker.spy(callback, "branch")
    fs.get(
        os.path.join("dir", "foo"),
        (tmp_dir / "foo").fs_path,
        callback=callback,
    )

    size = os.path.getsize(tmp_dir / "dir" / "foo")
    assert (tmp_dir / "foo").read_text() == "foo"
    assert callback.size == 1
    assert callback.value == 1

    assert branch.call_count == 1
    assert branch.spy_return.size == size
    assert branch.spy_return.value == size

    branch.reset_mock()

    callback = Callback()
    branch = mocker.spy(callback, "branch")
    fs.get(
        os.path.join("dir", "bar"),
        (tmp_dir / "bar").fs_path,
        callback=callback,
    )

    size = os.path.getsize(tmp_dir / "dir" / "bar")
    assert (tmp_dir / "bar").read_text() == "bar"
    assert callback.size == 1
    assert callback.value == 1

    assert branch.call_count == 1
    assert branch.spy_return.size == size
    assert branch.spy_return.value == size
