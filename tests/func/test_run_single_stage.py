import filecmp
import logging
import os
import textwrap
import uuid
from pathlib import Path

import pytest

from dvc.cli import main
from dvc.dependency.base import DependencyIsStageFileError
from dvc.dvcfile import DVC_FILE_SUFFIX
from dvc.exceptions import (
    ArgumentDuplicationError,
    CircularDependencyError,
    CyclicGraphError,
    OutputDuplicationError,
    OverlappingOutputPathsError,
    StagePathAsOutputError,
)
from dvc.output import OutputIsStageFileError
from dvc.stage import Stage
from dvc.stage.exceptions import (
    StageFileAlreadyExistsError,
    StageFileBadNameError,
    StagePathNotDirectoryError,
    StagePathNotFoundError,
    StagePathOutsideError,
)
from dvc.utils.serialize import load_yaml
from dvc_data.hashfile.hash import file_md5


def test_run(tmp_dir, copy_script, dvc):
    tmp_dir.gen("foo", "foo")
    cmd = "python copy.py foo out"
    deps = ["foo", "copy.py"]
    outs = [os.path.join(tmp_dir, "out")]
    outs_no_cache = []
    fname = "out.dvc"

    dvc.add("foo")
    stage = dvc.run(
        cmd=cmd,
        deps=deps,
        outs=outs,
        outs_no_cache=outs_no_cache,
        fname=fname,
        single_stage=True,
    )

    assert filecmp.cmp("foo", "out", shallow=False)
    assert os.path.isfile(stage.path)
    assert stage.cmd == cmd
    assert len(stage.deps) == len(deps)
    assert len(stage.outs) == len(outs + outs_no_cache)
    assert stage.outs[0].fspath == outs[0]
    assert stage.outs[0].hash_info.value == file_md5("foo")
    assert stage.path, fname

    with pytest.raises(OutputDuplicationError):
        dvc.run(
            cmd=cmd,
            deps=deps,
            outs=outs,
            outs_no_cache=outs_no_cache,
            fname="duplicate" + fname,
            single_stage=True,
        )


def test_run_empty(dvc):
    dvc.run(
        cmd="echo hello world",
        deps=[],
        outs=[],
        outs_no_cache=[],
        fname="empty.dvc",
        single_stage=True,
    )


def test_run_missing_dep(dvc):
    from dvc.dependency.base import DependencyDoesNotExistError

    with pytest.raises(DependencyDoesNotExistError):
        dvc.run(
            cmd="command",
            deps=["non-existing-dep"],
            outs=[],
            outs_no_cache=[],
            fname="empty.dvc",
            single_stage=True,
        )


def test_run_noexec(tmp_dir, dvc, scm):
    tmp_dir.gen("foo", "foo")
    dvc.run(
        cmd="cp foo bar",
        deps=["foo"],
        outs=["bar"],
        no_exec=True,
        single_stage=True,
    )
    assert not os.path.exists("bar")
    with open(".gitignore", encoding="utf-8") as fobj:
        assert fobj.read() == "/bar\n"


class TestRunCircularDependency:
    def test(self, dvc):
        with pytest.raises(CircularDependencyError):
            dvc.run(
                cmd="command",
                deps=["foo"],
                outs=["foo"],
                fname="circular-dependency.dvc",
                single_stage=True,
            )

    def test_outs_no_cache(self, dvc):
        with pytest.raises(CircularDependencyError):
            dvc.run(
                cmd="command",
                deps=["foo"],
                outs_no_cache=["foo"],
                fname="circular-dependency.dvc",
                single_stage=True,
            )

    def test_non_normalized_paths(self, dvc):
        with pytest.raises(CircularDependencyError):
            dvc.run(
                cmd="command",
                deps=["./foo"],
                outs=["foo"],
                fname="circular-dependency.dvc",
                single_stage=True,
            )

    def test_graph(self, tmp_dir, dvc):
        tmp_dir.gen("foo", "foo")
        dvc.run(
            deps=["foo"],
            outs=["bar.txt"],
            cmd="echo bar > bar.txt",
            single_stage=True,
        )

        dvc.run(
            deps=["bar.txt"],
            outs=["baz.txt"],
            cmd="echo baz > baz.txt",
            single_stage=True,
        )

        with pytest.raises(CyclicGraphError):
            dvc.run(
                deps=["baz.txt"],
                outs=["foo"],
                cmd="echo baz > foo",
                single_stage=True,
            )


class TestRunDuplicatedArguments:
    def test(self, dvc):
        with pytest.raises(ArgumentDuplicationError):
            dvc.run(
                cmd="command",
                deps=[],
                outs=["foo", "foo"],
                fname="circular-dependency.dvc",
                single_stage=True,
            )

    def test_outs_no_cache(self, dvc):
        with pytest.raises(ArgumentDuplicationError):
            dvc.run(
                cmd="command",
                outs=["foo"],
                outs_no_cache=["foo"],
                fname="circular-dependency.dvc",
                single_stage=True,
            )

    def test_non_normalized_paths(self, dvc):
        with pytest.raises(ArgumentDuplicationError):
            dvc.run(
                cmd="command",
                deps=[],
                outs=["foo", "./foo"],
                fname="circular-dependency.dvc",
                single_stage=True,
            )


class TestRunStageInsideOutput:
    def test_cwd(self, tmp_dir, dvc):
        tmp_dir.gen("data", {"foo": "foo", "bar": "bar"})
        dvc.run(
            cmd="mkdir data",
            deps=[],
            outs=["data"],
            single_stage=True,
        )

        with pytest.raises(StagePathAsOutputError):
            dvc.run(
                cmd="command",
                fname=os.path.join("data", "inside-cwd.dvc"),
                single_stage=True,
            )

    def test_file_name(self, tmp_dir, dvc):
        tmp_dir.gen("data", {"foo": "foo", "bar": "bar"})
        dvc.run(
            cmd="mkdir data",
            deps=[],
            outs=["data"],
            single_stage=True,
        )

        with pytest.raises(StagePathAsOutputError):
            dvc.run(
                cmd="command",
                outs=["foo"],
                fname=os.path.join("data", "inside-cwd.dvc"),
                single_stage=True,
            )


class TestRunBadCwd:
    def test(self, make_tmp_dir, dvc):
        with pytest.raises(StagePathOutsideError):
            dvc.run(
                cmd="command",
                wdir=make_tmp_dir("tmp"),
                fname="bad-cwd.dvc",
                single_stage=True,
            )

    def test_same_prefix(self, tmp_dir, dvc):
        path = f"{tmp_dir}-{uuid.uuid4()}"
        os.mkdir(path)
        with pytest.raises(StagePathOutsideError):
            dvc.run(cmd="command", wdir=path, fname="bad-cwd.dvc", single_stage=True)


class TestRunBadWdir:
    def test(self, make_tmp_dir, dvc):
        with pytest.raises(StagePathOutsideError):
            dvc.run(
                cmd="command",
                wdir=make_tmp_dir("tmp"),
                fname="bad-wdir.dvc",
                single_stage=True,
            )

    def test_same_prefix(self, tmp_dir, dvc):
        path = f"{tmp_dir}-{uuid.uuid4()}"
        os.mkdir(path)
        with pytest.raises(StagePathOutsideError):
            dvc.run(cmd="command", wdir=path, fname="bad-wdir.dvc", single_stage=True)

    def test_not_found(self, tmp_dir, dvc):
        path = os.path.join(tmp_dir, str(uuid.uuid4()))
        with pytest.raises(StagePathNotFoundError):
            dvc.run(cmd="command", wdir=path, fname="bad-wdir.dvc", single_stage=True)

    def test_not_dir(self, tmp_dir, dvc):
        path = tmp_dir / str(uuid.uuid4())
        path.mkdir()
        path = path / str(uuid.uuid4())
        path.touch()
        with pytest.raises(StagePathNotDirectoryError):
            dvc.run(
                cmd="command",
                wdir=os.fspath(path),
                fname="bad-wdir.dvc",
                single_stage=True,
            )


class TestRunBadName:
    def test(self, make_tmp_dir, dvc):
        with pytest.raises(StagePathOutsideError):
            dvc.run(
                cmd="command",
                fname=os.path.join(make_tmp_dir("tmp"), "foo.dvc"),
                single_stage=True,
            )

    def test_same_prefix(self, tmp_dir, dvc):
        path = f"{tmp_dir}-{uuid.uuid4()}"
        os.mkdir(path)
        with pytest.raises(StagePathOutsideError):
            dvc.run(
                cmd="command",
                fname=os.path.join(path, "foo.dvc"),
                single_stage=True,
            )

    def test_not_found(self, tmp_dir, dvc):
        path = os.path.join(tmp_dir, str(uuid.uuid4()))
        with pytest.raises(StagePathNotFoundError):
            dvc.run(
                cmd="command",
                fname=os.path.join(path, "foo.dvc"),
                single_stage=True,
            )


def test_run_remove_outs(tmp_dir, dvc, append_foo_script):
    dvc.run(
        deps=["append_foo.py"],
        outs=["foo"],
        cmd="python append_foo.py foo",
        single_stage=True,
    )


class TestCmdRunWorkingDirectory:
    def test_default_wdir_is_not_written(self, tmp_dir, dvc):
        stage = dvc.run(
            cmd="echo test > foo",
            outs=["foo"],
            wdir=".",
            single_stage=True,
        )
        d = load_yaml(stage.relpath)
        assert Stage.PARAM_WDIR not in d.keys()

        stage = dvc.run(cmd="echo test > bar", outs=["bar"], single_stage=True)
        d = load_yaml(stage.relpath)
        assert Stage.PARAM_WDIR not in d.keys()

    def test_fname_changes_path_and_wdir(self, tmp_dir, dvc):
        dname = "dir"
        os.mkdir(os.path.join(tmp_dir, dname))
        foo = os.path.join(dname, "foo")
        fname = os.path.join(dname, "stage" + DVC_FILE_SUFFIX)
        stage = dvc.run(
            cmd=f"echo test > {foo}",
            outs=[foo],
            fname=fname,
            single_stage=True,
        )
        assert stage.wdir == os.path.realpath(tmp_dir)
        assert stage.path == os.path.join(os.path.realpath(tmp_dir), fname)

        # Check that it is dumped properly (relative to fname)
        d = load_yaml(stage.relpath)
        assert d[Stage.PARAM_WDIR] == ".."


def test_rerun_deterministic(tmp_dir, run_copy, mocker):
    from dvc.stage.run import subprocess

    tmp_dir.gen("foo", "foo content")

    spy = mocker.spy(subprocess, "Popen")

    run_copy("foo", "out", single_stage=True)
    assert spy.called

    spy.reset_mock()
    run_copy("foo", "out", single_stage=True)
    assert not spy.called


def test_rerun_deterministic_ignore_cache(tmp_dir, run_copy, mocker):
    from dvc.stage.run import subprocess

    tmp_dir.gen("foo", "foo content")

    spy = mocker.spy(subprocess, "Popen")

    run_copy("foo", "out", single_stage=True)
    assert spy.called

    spy.reset_mock()
    run_copy("foo", "out", run_cache=False, single_stage=True)
    assert spy.called


def test_rerun_callback(dvc):
    def run_callback(force=False):
        return dvc.run(
            cmd="echo content > out", force=force, fname="echo.dvc", single_stage=True
        )

    assert run_callback() is not None
    with pytest.raises(StageFileAlreadyExistsError):
        assert run_callback() is not None
    assert run_callback(force=True) is not None


def test_rerun_changed_dep(tmp_dir, run_copy):
    tmp_dir.gen("foo", "foo content")
    assert run_copy("foo", "out", single_stage=True) is not None

    tmp_dir.gen("foo", "changed content")
    with pytest.raises(StageFileAlreadyExistsError):
        run_copy("foo", "out", force=False, single_stage=True)
    assert run_copy("foo", "out", force=True, single_stage=True)


def test_rerun_changed_stage(tmp_dir, run_copy):
    tmp_dir.gen("foo", "foo content")
    assert run_copy("foo", "out", single_stage=True) is not None

    tmp_dir.gen("bar", "bar content")
    with pytest.raises(StageFileAlreadyExistsError):
        run_copy("bar", "out", force=False, single_stage=True)


def test_rerun_changed_out(tmp_dir, run_copy):
    tmp_dir.gen("foo", "foo content")
    assert run_copy("foo", "out", single_stage=True) is not None

    Path("out").write_text("modification", encoding="utf-8")
    with pytest.raises(StageFileAlreadyExistsError):
        run_copy("foo", "out", force=False, single_stage=True)


def test_should_raise_on_overlapping_output_paths(tmp_dir, dvc, append_foo_script):
    tmp_dir.gen("data", {"foo": "foo", "bar": "bar"})
    ret = main(["add", "data"])
    assert ret == 0

    foo_file = os.path.join("data", "foo")
    with pytest.raises(OverlappingOutputPathsError) as err:
        dvc.run(
            outs=["data/foo"],
            cmd=f"python append_foo.py {foo_file}",
            single_stage=True,
        )

    error_output = str(err.value)

    assert "The output paths:\n" in error_output
    assert "\n'data'('data.dvc')\n" in error_output
    assert f"\n'{foo_file}'('foo.dvc')\n" in error_output
    assert (
        "overlap and are thus in the same tracked directory.\n"
        "To keep reproducibility, outputs should be in separate "
        "tracked directories or tracked individually." in error_output
    )


def test_should_not_checkout_upon_corrupted_local_hardlink_cache(
    mocker, tmp_dir, dvc, copy_script
):
    tmp_dir.gen("foo", "foo")
    dvc.cache.local.cache_types = ["hardlink"]

    stage = dvc.run(
        deps=["foo"],
        outs=["bar"],
        cmd="python copy.py foo bar",
        single_stage=True,
    )

    os.chmod("bar", 0o644)
    with open("bar", "w", encoding="utf-8") as fd:
        fd.write("corrupting the output cache")

    spy_checkout = mocker.spy(stage.outs[0], "checkout")
    from dvc.stage import run as stage_run

    spy_run = mocker.spy(stage_run, "cmd_run")

    with dvc.lock:
        stage.run()

        spy_run.assert_called_once()
        spy_checkout.assert_not_called()


def test_bad_stage_fname(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo content")

    with pytest.raises(StageFileBadNameError):
        # fname should end with .dvc
        run_copy("foo", "foo_copy", fname="out_stage", single_stage=True)

    # Check that command hasn't been run
    assert not (tmp_dir / "foo_copy").exists()


def test_should_raise_on_stage_dependency(run_copy):
    with pytest.raises(DependencyIsStageFileError):
        run_copy("name.dvc", "stage_copy", single_stage=True)


def test_should_raise_on_stage_output(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo content")

    with pytest.raises(OutputIsStageFileError):
        run_copy("foo", "name.dvc", single_stage=True)


@pytest.mark.parametrize("metrics_type", ["metrics", "metrics_no_cache"])
def test_metrics_dir(tmp_dir, dvc, caplog, run_copy_metrics, metrics_type):
    copyargs = {metrics_type: ["dir_metric"]}
    tmp_dir.gen({"dir": {"file": "content"}})
    with caplog.at_level(logging.DEBUG, "dvc"):
        run_copy_metrics("dir", "dir_metric", **copyargs)
    assert "directory 'dir_metric' cannot be used as metrics." in caplog.messages


def test_run_force_preserves_comments_and_meta(tmp_dir, dvc, run_copy):
    tmp_dir.gen({"foo": "foo", "foo1": "foo1"})
    text = textwrap.dedent(
        """\
      desc: top desc
      cmd: python copy.py foo bar
      deps:
      - path: copy.py
      - path: foo
      outs:
      # comment preserved
      - path: bar
        desc: out desc
        type: mytype
        labels:
        - label1
        - label2
        meta:
          key: value
      meta:
        name: copy-foo-bar
    """
    )
    (tmp_dir / "bar.dvc").write_text(text)
    dvc.reproduce("bar.dvc")

    # CRLF on windows makes the generated file bigger in size
    code_size = 176 if os.name == "nt" else 167
    assert (tmp_dir / "bar.dvc").read_text() == textwrap.dedent(
        f"""\
        desc: top desc
        cmd: python copy.py foo bar
        deps:
        - path: copy.py
          md5: a618d3a2c3d5a35f0aa4707951d986f5
          size: {code_size}
        - path: foo
          md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
        outs:
        # comment preserved
        - path: bar
          desc: out desc
          type: mytype
          labels:
          - label1
          - label2
          meta:
            key: value
          md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
        meta:
          name: copy-foo-bar
        md5: 262f10a31f4b218b7b450b3511c2413f
    """
    )

    run_copy("foo1", "bar1", single_stage=True, force=True, fname="bar.dvc")
    assert (tmp_dir / "bar.dvc").read_text() == textwrap.dedent(
        f"""\
        desc: top desc
        cmd: python copy.py foo1 bar1
        deps:
        - path: foo1
          md5: 299a0be4a5a79e6a59fdd251b19d78bb
          size: 4
        - path: copy.py
          md5: a618d3a2c3d5a35f0aa4707951d986f5
          size: {code_size}
        outs:
        # comment preserved
        - path: bar1
          md5: 299a0be4a5a79e6a59fdd251b19d78bb
          size: 4
        meta:
          name: copy-foo-bar
        md5: 61a1506995d7550a366b80d2301530b7
    """
    )
