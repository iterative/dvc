import logging
import os
import textwrap
import uuid

import pytest
from funcy import get_in

from dvc.cli import main
from dvc.dependency import ParamsDependency
from dvc.dependency.base import DependencyDoesNotExistError
from dvc.dvcfile import LOCK_FILE, PROJECT_FILE, load_file
from dvc.exceptions import (
    ArgumentDuplicationError,
    CircularDependencyError,
    CyclicGraphError,
    InvalidArgumentError,
    OutputDuplicationError,
    OverlappingOutputPathsError,
)
from dvc.stage import PipelineStage
from dvc.stage.exceptions import (
    DuplicateStageName,
    InvalidStageName,
    StagePathNotDirectoryError,
    StagePathNotFoundError,
    StagePathOutsideError,
)
from dvc.utils.serialize import load_yaml


def test_run(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen("foo", "foo")
    assert not os.path.exists(PROJECT_FILE)
    stage = dvc.run(
        cmd="python copy.py foo bar",
        deps=["foo", "copy.py"],
        outs=["bar"],
        name="copy-foo-to-bar",
    )
    assert isinstance(stage, PipelineStage)
    assert stage.name == "copy-foo-to-bar"
    assert os.path.exists(PROJECT_FILE)
    assert os.path.exists(LOCK_FILE)
    assert stage.cmd == "python copy.py foo bar"
    assert len(stage.deps) == 2
    assert len(stage.outs) == 1

    with pytest.raises(OutputDuplicationError):
        dvc.run(
            cmd="python copy.py foo bar",
            deps=["foo", "copy.py"],
            outs=["bar"],
            name="duplicate",
        )


def test_run_empty(dvc):
    dvc.run(cmd="echo hello world", deps=[], outs=[], outs_no_cache=[], name="empty")


def test_run_missing_dep(dvc):
    with pytest.raises(DependencyDoesNotExistError):
        dvc.run(
            cmd="command",
            deps=["non-existing-dep"],
            outs=[],
            outs_no_cache=[],
            name="missing-dep",
        )


def test_run_no_exec(tmp_dir, dvc, scm, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    assert not os.path.exists(PROJECT_FILE)
    stage = run_copy("foo", "bar", name="copy-foo-to-bar", no_exec=True)
    assert isinstance(stage, PipelineStage)
    assert stage.name == "copy-foo-to-bar"
    assert os.path.exists(PROJECT_FILE)
    assert not os.path.exists(LOCK_FILE)
    assert not os.path.exists("bar")

    data, _ = stage.dvcfile._load()
    assert data["stages"]["copy-foo-to-bar"] == {
        "cmd": "python copy.py foo bar",
        "deps": ["copy.py", "foo"],
        "outs": ["bar"],
    }
    with open(".gitignore", encoding="utf-8") as fobj:
        assert fobj.read() == "/foo\n/bar\n"


def test_run_repeat(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import PROJECT_FILE, load_file
    from dvc.stage import PipelineStage

    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "foo1", name="copy-foo-foo1")
    run_copy("foo1", "foo2", name="copy-foo1-foo2")
    run_copy("foo2", "foo3", name="copy-foo2-foo3")

    stages = list(load_file(dvc, PROJECT_FILE).stages.values())
    assert len(stages) == 3
    assert all(isinstance(stage, PipelineStage) for stage in stages)
    assert {stage.name for stage in stages} == {
        "copy-foo-foo1",
        "copy-foo1-foo2",
        "copy-foo2-foo3",
    }


def test_run_cached(tmp_dir, dvc, run_copy, mocker):
    from dvc.stage.run import subprocess

    tmp_dir.dvc_gen("foo", "foo")

    run_copy("foo", "foo2", name="copy-foo1-foo2")
    spy = mocker.spy(subprocess, "Popen")
    run_copy("foo", "foo2", name="copy-foo1-foo2")
    assert not spy.called


def test_dump_on_non_cached_outputs(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo")
    dvc.run(
        cmd="cp foo foo1",
        deps=["foo"],
        name="copy-foo1-foo2",
        outs_no_cache=["foo1"],
    )


def test_with_wdir(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    stage = dvc.run(
        cmd="cp foo foo1",
        deps=["foo"],
        name="copy-foo1-foo2",
        outs=["foo1"],
        wdir="dir",
    )

    data, _ = load_file(dvc, stage.path)._load()
    assert data["stages"]["copy-foo1-foo2"]["wdir"] == "dir"


def test_always_changed(tmp_dir, dvc):
    from dvc.dvcfile import load_file

    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    stage = dvc.run(
        cmd="cp foo foo1",
        deps=["foo"],
        name="copy-foo1-foo2",
        outs=["foo1"],
        always_changed=True,
    )

    data, _ = load_file(dvc, stage.path)._load()
    assert data["stages"]["copy-foo1-foo2"]["always_changed"]


def test_graph(tmp_dir, dvc):
    from dvc.exceptions import CyclicGraphError

    tmp_dir.gen({"foo": "foo", "bar": "bar"})

    dvc.run(deps=["foo"], outs=["bar"], cmd="echo foo > bar", name="1")

    dvc.run(deps=["bar"], outs=["baz"], cmd="echo bar > baz", name="2")

    with pytest.raises(CyclicGraphError):
        dvc.run(deps=["baz"], outs=["foo"], cmd="echo baz > foo", name="3")


class TestRunCircularDependency:
    def test(self, dvc):
        with pytest.raises(CircularDependencyError):
            dvc.run(
                cmd="command",
                deps=["foo"],
                outs=["foo"],
                name="circular-dependency",
            )

    def test_outs_no_cache(self, dvc):
        with pytest.raises(CircularDependencyError):
            dvc.run(
                cmd="command",
                deps=["foo"],
                outs_no_cache=["foo"],
                name="circular-dependency",
            )

    def test_non_normalized_paths(self, dvc):
        with pytest.raises(CircularDependencyError):
            dvc.run(
                cmd="command",
                deps=["./foo"],
                outs=["foo"],
                name="circular-dependency",
            )

    def test_graph(self, tmp_dir, dvc):
        tmp_dir.gen("foo", "foo")
        dvc.run(
            deps=["foo"],
            outs=["bar.txt"],
            cmd="echo bar > bar.txt",
            name="gen-bar-txt",
        )

        dvc.run(
            deps=["bar.txt"],
            outs=["baz.txt"],
            cmd="echo baz > baz.txt",
            name="gen-baz-txt",
        )

        with pytest.raises(CyclicGraphError):
            dvc.run(
                deps=["baz.txt"],
                outs=["foo"],
                cmd="echo baz > foo",
                name="gen-foo",
            )


class TestRunDuplicatedArguments:
    def test(self, dvc):
        with pytest.raises(ArgumentDuplicationError):
            dvc.run(
                cmd="command",
                deps=[],
                outs=["foo", "foo"],
                name="circular-dependency",
            )

    def test_outs_no_cache(self, dvc):
        with pytest.raises(ArgumentDuplicationError):
            dvc.run(
                cmd="command",
                outs=["foo"],
                outs_no_cache=["foo"],
                name="circular-dependency",
            )

    def test_non_normalized_paths(self, dvc):
        with pytest.raises(ArgumentDuplicationError):
            dvc.run(
                cmd="command",
                deps=[],
                outs=["foo", "./foo"],
                name="circular-dependency",
            )


class TestRunBadWdir:
    def test(self, make_tmp_dir, dvc):
        with pytest.raises(StagePathOutsideError):
            dvc.run(cmd="command", wdir=make_tmp_dir("tmp"), name="bad-wdir")

    def test_same_prefix(self, tmp_dir, dvc):
        path = f"{tmp_dir}-{uuid.uuid4()}"
        os.mkdir(path)
        with pytest.raises(StagePathOutsideError):
            dvc.run(cmd="command", wdir=path, name="bad-wdir")

    def test_not_found(self, tmp_dir, dvc):
        path = os.path.join(tmp_dir, str(uuid.uuid4()))
        with pytest.raises(StagePathNotFoundError):
            dvc.run(cmd="command", wdir=path, name="bad-wdir")

    def test_not_dir(self, tmp_dir, dvc):
        path = tmp_dir / str(uuid.uuid4())
        path.mkdir()
        path = path / str(uuid.uuid4())
        path.touch()
        with pytest.raises(StagePathNotDirectoryError):
            dvc.run(cmd="command", wdir=os.fspath(path), name="bad-wdir")


class TestCmdRunWorkingDirectory:
    def test_default_wdir_is_not_written(self, tmp_dir, dvc):
        dvc.run(cmd="echo test > foo", outs=["foo"], wdir=".", name="echo-foo")

        d = load_yaml("dvc.yaml")
        assert "wdir" not in get_in(d, ["stages", "echo-foo"])

        dvc.run(cmd="echo test > bar", outs=["bar"], name="echo-bar")
        d = load_yaml("dvc.yaml")
        assert "wdir" not in get_in(d, ["stages", "echo-bar"])

    def test_fname_changes_path_and_wdir(self, tmp_dir, dvc):
        dirpath = tmp_dir / "dir"
        dirpath.mkdir()

        with dirpath.chdir():
            stage = dvc.run(
                cmd="echo test > foo",
                outs=["foo"],
                wdir=os.fspath(tmp_dir),
                name="echo",
            )
        assert stage.wdir == os.path.realpath(tmp_dir)

        # Check that it is dumped properly
        d = load_yaml("dir/dvc.yaml")
        assert get_in(d, ["stages", "echo", "wdir"]) == ".."


def test_run_dump(tmp_dir, dvc, run_head):
    from dvc.dvcfile import load_file

    tmp_dir.gen(
        {
            "dir": {
                "foo": "foo\nfoo",
                "bar": "bar\nbar",
                "foobar": "foobar\foobar",
            }
        }
    )

    dvc.run(
        cmd="cp foo foo2",
        deps=["foo"],
        name="copy-foo-foo2",
        wdir="dir",
        outs_persist=["foo2"],
        always_changed=True,
    )
    data = load_file(dvc, PROJECT_FILE)._load()[0]
    assert data == {
        "stages": {
            "copy-foo-foo2": {
                "cmd": "cp foo foo2",
                "deps": ["foo"],
                "outs": [{"foo2": {"persist": True}}],
                "always_changed": True,
                "wdir": "dir",
            }
        }
    }

    run_head(
        "foo",
        "bar",
        "foobar",
        name="head-files",
        outs=["bar-1"],
        outs_persist=["foo-1"],
        metrics_no_cache=["foobar-1"],
        wdir="dir",
    )
    assert load_file(dvc, PROJECT_FILE)._load()[0] == {
        "stages": {
            "head-files": {
                "cmd": "python {} foo bar foobar".format(
                    (tmp_dir / "head.py").resolve()
                ),
                "wdir": "dir",
                "deps": ["bar", "foo", "foobar"],
                "outs": ["bar-1", {"foo-1": {"persist": True}}],
                "metrics": [{"foobar-1": {"cache": False}}],
            },
            **data["stages"],
        }
    }


@pytest.mark.parametrize("char", ["@:", "#", "$", ":", "/", "\\", ".", ";", ","])
def test_run_with_invalid_stage_name(run_copy, char):
    with pytest.raises(InvalidStageName):
        run_copy("foo", "bar", name=f"copy_name-{char}")


def test_run_with_name_having_hyphen_underscore(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo_bar")


def test_run_already_exists(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy")
    with pytest.raises(DuplicateStageName):
        run_copy("bar", "foobar", name="copy", force=False)
    run_copy("bar", "foobar", name="copy", force=True)


supported_params = {
    "name": "Answer",
    "answer": 42,
    "floats": 42.0,
    "lists": [42, 42.0, "42"],
    "nested": {"nested1": {"nested2": "42", "nested2-2": 41.99999}},
}


def test_run_params_default(tmp_dir, dvc):
    (tmp_dir / "params.yaml").dump(supported_params)
    stage = dvc.run(
        name="read_params",
        params=["nested.nested1.nested2"],
        cmd="cat params.yaml",
    )
    assert isinstance(stage.deps[0], ParamsDependency)
    assert stage.deps[0].params == ["nested.nested1.nested2"]

    lockfile = stage.dvcfile._lockfile
    assert lockfile.load()["stages"]["read_params"]["params"] == {
        "params.yaml": {"nested.nested1.nested2": "42"}
    }

    data, _ = stage.dvcfile._load()
    assert data["stages"]["read_params"]["params"] == ["nested.nested1.nested2"]


def test_run_params_custom_file(tmp_dir, dvc):
    (tmp_dir / "params2.yaml").dump(supported_params)
    stage = dvc.run(
        name="read_params",
        params=["params2.yaml:lists"],
        cmd="cat params2.yaml",
    )

    isinstance(stage.deps[0], ParamsDependency)
    assert stage.deps[0].params == ["lists"]
    lockfile = stage.dvcfile._lockfile
    assert lockfile.load()["stages"]["read_params"]["params"] == {
        "params2.yaml": {"lists": [42, 42.0, "42"]}
    }

    data, _ = stage.dvcfile._load()
    assert data["stages"]["read_params"]["params"] == [{"params2.yaml": ["lists"]}]


def test_run_params_no_exec(tmp_dir, dvc):
    (tmp_dir / "params2.yaml").dump(supported_params)
    stage = dvc.run(
        name="read_params",
        params=["params2.yaml:lists"],
        cmd="cat params2.yaml",
        no_exec=True,
    )

    isinstance(stage.deps[0], ParamsDependency)
    assert stage.deps[0].params == ["lists"]
    assert not stage.dvcfile._lockfile.exists()

    data, _ = stage.dvcfile._load()
    assert data["stages"]["read_params"]["params"] == [{"params2.yaml": ["lists"]}]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"outs": ["foo"], "deps": ["bar"]},
        {"outs": ["foo"], "deps": ["bar"], "name": "copy-foo-bar"},
    ],
)
def test_run_without_cmd(tmp_dir, dvc, kwargs):
    with pytest.raises(InvalidArgumentError) as exc:
        dvc.run(**kwargs)
    assert str(exc.value) == "command is not specified"


def test_run_overwrite_order(tmp_dir, dvc, run_copy):
    tmp_dir.gen({"foo": "foo", "foo1": "foo1"})
    run_copy("foo", "bar", name="copy-foo-bar")
    run_copy("bar", "foobar", name="copy-bar-foobar")

    run_copy("foo1", "bar1", name="copy-foo-bar", force=True)

    data = (tmp_dir / PROJECT_FILE).parse()
    assert list(data["stages"].keys()) == ["copy-foo-bar", "copy-bar-foobar"]


def test_run_overwrite_preserves_meta_and_comment(tmp_dir, dvc, run_copy):
    tmp_dir.gen({"foo": "foo", "foo1": "foo1"})
    text = textwrap.dedent(
        """\
        stages:
          copy-foo-bar:
            cmd: python copy.py {src} {dest}
            deps:
            - copy.py
            - {src}
            outs:
            # comments are preserved
            - {dest}
            meta:
              name: meta is preserved too
    """
    )
    (tmp_dir / PROJECT_FILE).write_text(text.format(src="foo", dest="bar"))
    assert dvc.reproduce(PROJECT_FILE)

    assert run_copy("foo1", "bar1", name="copy-foo-bar", force=True)

    assert (tmp_dir / PROJECT_FILE).read_text() == text.format(src="foo1", dest="bar1")


def test_run_external_outputs(tmp_dir, dvc, local_workspace):
    hash_name = "md5"
    foo_hash = "acbd18db4cc2f85cedef654fccc4a4d8"
    bar_hash = "37b51d194a7513e45b56f6524f2d51f2"

    local_workspace.gen("foo", "foo")
    dvc.run(
        name="mystage",
        cmd="mycmd",
        deps=["remote://workspace/foo"],
        outs_no_cache=["remote://workspace/bar"],
        no_exec=True,
    )

    dvc_yaml = (
        "stages:\n"
        "  mystage:\n"
        "    cmd: mycmd\n"
        "    deps:\n"
        "    - remote://workspace/foo\n"
        "    outs:\n"
        "    - remote://workspace/bar:\n"
        "        cache: false\n"
    )

    assert (tmp_dir / "dvc.yaml").read_text() == dvc_yaml
    assert not (tmp_dir / "dvc.lock").exists()

    local_workspace.gen("bar", "bar")
    dvc.commit("dvc.yaml", force=True)

    assert (tmp_dir / "dvc.yaml").read_text() == dvc_yaml
    assert (tmp_dir / "dvc.lock").read_text() == (
        "schema: '2.0'\n"
        "stages:\n"
        "  mystage:\n"
        "    cmd: mycmd\n"
        "    deps:\n"
        "    - path: remote://workspace/foo\n"
        "      hash: md5\n"
        f"      {hash_name}: {foo_hash}\n"
        "      size: 3\n"
        "    outs:\n"
        "    - path: remote://workspace/bar\n"
        "      hash: md5\n"
        f"      {hash_name}: {bar_hash}\n"
        "      size: 3\n"
    )

    assert (local_workspace / "foo").read_text() == "foo"
    assert (local_workspace / "bar").read_text() == "bar"
    assert not (local_workspace / "cache").exists()


def test_rerun_callback(dvc):
    def run_callback(force=False):
        return dvc.run(cmd="echo content > out", force=force, name="echo")

    assert run_callback() is not None
    with pytest.raises(DuplicateStageName):
        assert run_callback() is not None
    assert run_callback(force=True) is not None


def test_rerun_changed_dep(tmp_dir, run_copy):
    tmp_dir.gen("foo", "foo content")
    assert run_copy("foo", "out", name="copy") is not None

    tmp_dir.gen("foo", "changed content")
    with pytest.raises(DuplicateStageName):
        run_copy("foo", "out", force=False, name="copy")
    assert run_copy("foo", "out", force=True, name="copy")


def test_run_remove_outs(tmp_dir, dvc, append_foo_script):
    tmp_dir.gen("foo", "foo")
    dvc.run(
        deps=["append_foo.py"],
        outs=["foo"],
        cmd="python append_foo.py foo",
        name="append-foo",
    )


@pytest.mark.parametrize("metrics_type", ["metrics", "metrics_no_cache"])
def test_metrics_dir(tmp_dir, dvc, caplog, run_copy_metrics, metrics_type):
    copyargs = {metrics_type: ["dir_metric"]}
    tmp_dir.gen({"dir": {"file": "content"}})
    with caplog.at_level(logging.DEBUG, "dvc"):
        run_copy_metrics("dir", "dir_metric", name="copy-metrics", **copyargs)
    assert "directory 'dir_metric' cannot be used as metrics." in caplog.messages


def test_rerun_deterministic(tmp_dir, run_copy, mocker):
    from dvc.stage.run import subprocess

    tmp_dir.gen("foo", "foo content")

    spy = mocker.spy(subprocess, "Popen")

    run_copy("foo", "out", name="copy")
    assert spy.called

    spy.reset_mock()
    run_copy("foo", "out", name="copy")
    assert not spy.called


def test_rerun_deterministic_ignore_cache(tmp_dir, run_copy, mocker):
    from dvc.stage.run import subprocess

    tmp_dir.gen("foo", "foo content")

    spy = mocker.spy(subprocess, "Popen")

    run_copy("foo", "out", name="copy")
    assert spy.called

    spy.reset_mock()
    run_copy("foo", "out", run_cache=False, name="copy")
    assert spy.called


def test_rerun_changed_stage(tmp_dir, run_copy):
    tmp_dir.gen("foo", "foo content")
    assert run_copy("foo", "out", name="copy") is not None

    tmp_dir.gen("bar", "bar content")
    with pytest.raises(DuplicateStageName):
        run_copy("bar", "out", force=False, name="copy")


def test_rerun_changed_out(tmp_dir, run_copy):
    tmp_dir.gen("foo", "foo content")
    assert run_copy("foo", "out", name="copy") is not None

    tmp_dir.gen("out", "modification")
    with pytest.raises(DuplicateStageName):
        run_copy("foo", "out", force=False, name="copy")


def test_should_raise_on_overlapping_output_paths(tmp_dir, dvc, append_foo_script):
    tmp_dir.gen("data", {"foo": "foo", "bar": "bar"})
    ret = main(["add", "data"])
    assert ret == 0

    foo_file = os.path.join("data", "foo")
    with pytest.raises(OverlappingOutputPathsError) as err:
        dvc.run(
            outs=["data/foo"],
            cmd=f"python append_foo.py {foo_file}",
            name="append-foo",
        )

    error_output = str(err.value)

    assert "The output paths:\n" in error_output
    assert "\n'data'('data.dvc')\n" in error_output
    assert f"\n'{foo_file}'('append-foo')\n" in error_output
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
        name="copy",
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
