import json
import os

import pytest


@pytest.mark.parametrize(
    "stage_wdir, cwd, target",
    [
        (os.curdir, os.curdir, "foo"),
        (os.curdir, os.curdir, "train"),
        (os.curdir, os.curdir, "dvc.yaml:train"),
        (os.curdir, "sub", os.path.join(os.pardir, "foo")),
        (
            os.curdir,
            "sub",
            os.path.join(os.pardir, "dvc.yaml:train"),
        ),
        ("sub", os.curdir, os.path.join("sub", "foo")),
        ("sub", os.curdir, os.path.join("sub", "dvc.yaml:train")),
        ("sub", "sub", "foo"),
        ("sub", "sub", "train"),
        ("sub", "sub", "dvc.yaml:train"),
        ("sub", "dir", os.path.join(os.pardir, "sub", "foo")),
        (
            "sub",
            "dir",
            os.path.join(os.pardir, "sub", "dvc.yaml:train"),
        ),
    ],
)
def test_from_gitfs_when_pwd_not_in_root(tmp_dir, scm, dvc, stage_wdir, cwd, target):
    path = tmp_dir.joinpath(stage_wdir).resolve()
    path.mkdir(parents=True, exist_ok=True)
    wdir = tmp_dir.joinpath(cwd).resolve()
    wdir.mkdir(parents=True, exist_ok=True)

    (path / "dvc.yaml").write_text(
        json.dumps({"stages": {"train": {"cmd": "echo foo > foo", "outs": ["foo"]}}})
    )
    path.gen({"foo": "foo"})
    dvc.commit(None, force=True)
    tmp_dir.scm_add(
        [path / file for file in ("dvc.yaml", "dvc.lock", ".gitignore")],
        commit="add files",
    )

    with wdir.chdir():
        assert dvc.used_objs([target], revs=[scm.get_rev()])


def test_used_objs_push(tmp_dir, scm, dvc):
    stage = tmp_dir.dvc_gen("foo", "foo")[0]
    hash_info = stage.outs[0].hash_info

    stage.outs[0].can_push = True
    assert stage.get_used_objs(push=False) == {None: {hash_info}}
    assert stage.get_used_objs(push=True) == {None: {hash_info}}

    stage.outs[0].can_push = False
    assert stage.get_used_objs(push=False) == {None: {hash_info}}
    assert stage.get_used_objs(push=True) == {}
