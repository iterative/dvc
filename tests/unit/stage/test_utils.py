import os

from dvc.fs import localfs
from dvc.stage.utils import _get_stage_files, resolve_paths


def test_resolve_paths():
    p = os.path.join("dir", "subdir")
    file_path = os.path.join(p, "dvc.yaml")

    path, wdir = resolve_paths(fs=localfs, path=file_path, wdir="dir")
    assert path == os.path.abspath(file_path)
    assert wdir == os.path.abspath(os.path.join(p, "dir"))

    path, wdir = resolve_paths(fs=localfs, path=file_path)
    assert path == os.path.abspath(file_path)
    assert wdir == os.path.abspath(p)

    path, wdir = resolve_paths(fs=localfs, path=file_path, wdir="../../some-dir")
    assert path == os.path.abspath(file_path)
    assert wdir == os.path.abspath("some-dir")


def test_get_stage_files(tmp_dir, dvc):
    tmp_dir.dvc_gen("dvc-dep", "dvc-dep")
    tmp_dir.gen("other-dep", "other-dep")
    stage = dvc.stage.create(
        name="stage",
        cmd="foo",
        deps=["dvc-dep", "other-dep"],
        outs=["dvc-out"],
        outs_no_cache=["other-out"],
    )
    assert _get_stage_files(stage) == [
        "dvc.yaml",
        "dvc.lock",
        "other-dep",
        "other-out",
    ]


def test_get_stage_files_wdir(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"dvc-dep": "dvc-dep", "other-dep": "other-dep"}})
    dvc.add(os.path.join("dir", "dvc-dep"))
    stage = dvc.stage.create(
        name="stage",
        cmd="foo",
        wdir="dir",
        deps=["dvc-dep", "other-dep"],
        outs=["dvc-out"],
        outs_no_cache=["other-out"],
    )
    assert _get_stage_files(stage) == [
        "dvc.yaml",
        "dvc.lock",
        os.path.join("dir", "other-dep"),
        os.path.join("dir", "other-out"),
    ]
