import os

from dvc.repo.reproduce import _get_stage_files


def test_number_reproduces(tmp_dir, dvc, mocker):
    reproduce_stage_mock = mocker.patch(
        "dvc.repo.reproduce._reproduce_stage", returns=[]
    )
    tmp_dir.dvc_gen({"pre-foo": "pre-foo"})

    dvc.run(single_stage=True, deps=["pre-foo"], outs=["foo"], cmd="echo foo > foo")
    dvc.run(single_stage=True, deps=["foo"], outs=["bar"], cmd="echo bar > bar")
    dvc.run(single_stage=True, deps=["foo"], outs=["baz"], cmd="echo baz > baz")
    dvc.run(single_stage=True, deps=["bar"], outs=["boop"], cmd="echo boop > boop")

    reproduce_stage_mock.reset_mock()

    dvc.reproduce(all_pipelines=True)

    assert reproduce_stage_mock.call_count == 5


def test_get_stage_files(tmp_dir, dvc):
    tmp_dir.dvc_gen("dvc-dep", "dvc-dep")
    tmp_dir.gen("other-dep", "other-dep")

    stage = dvc.stage.add(
        name="stage",
        cmd="foo",
        deps=["dvc-dep", "other-dep"],
        outs=["dvc-out"],
        outs_no_cache=["other-out"],
    )
    result = set(_get_stage_files(stage))
    assert result == {
        stage.dvcfile.relpath,
        str(tmp_dir / "other-dep"),
        str(tmp_dir / "other-out"),
    }


def test_get_stage_files_wdir(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"dvc-dep": "dvc-dep", "other-dep": "other-dep"}})
    dvc.add(os.path.join("dir", "dvc-dep"))

    stage = dvc.stage.add(
        name="stage",
        cmd="foo",
        wdir="dir",
        deps=["dvc-dep", "other-dep"],
        outs=["dvc-out"],
        outs_no_cache=["other-out"],
    )
    result = set(_get_stage_files(stage))
    assert result == {
        stage.dvcfile.relpath,
        str(tmp_dir / "dir" / "other-dep"),
        str(tmp_dir / "dir" / "other-out"),
    }
