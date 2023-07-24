import filecmp
import os
import shutil
from copy import deepcopy
from textwrap import dedent

import pytest
from funcy import lsplit

from dvc.cli import main
from dvc.dvcfile import LOCK_FILE, PROJECT_FILE
from dvc.exceptions import CyclicGraphError, ReproductionError
from dvc.fs import system
from dvc.output import Output
from dvc.stage import PipelineStage, Stage
from dvc.stage.cache import RunCacheNotSupported
from dvc.stage.exceptions import StageFileDoesNotExistError, StageNotFound
from dvc.utils.fs import remove
from dvc.utils.serialize import modify_yaml
from dvc_data.hashfile.hash import file_md5


def test_non_existing_stage_name(tmp_dir, dvc, run_copy):
    tmp_dir.gen("file1", "file1")
    run_copy("file1", "file2", name="copy-file1-file2")

    with pytest.raises(StageNotFound):
        dvc.freeze(":copy-file1-file3")

    assert main(["freeze", ":copy-file1-file3"]) != 0


def test_repro_fail(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    os.unlink("copy.py")
    assert main(["repro", stage.addressing]) != 0


def test_repro_frozen(tmp_dir, dvc, run_copy):
    (data_stage,) = tmp_dir.dvc_gen("data", "foo")
    stage0 = run_copy("data", "stage0", name="copy-data-stage0")
    run_copy("stage0", "stage1", name="copy-data-stage1")
    run_copy("stage1", "stage2", name="copy-data-stage2")

    dvc.freeze("copy-data-stage1")

    tmp_dir.gen("data", "bar")
    stages = dvc.reproduce()
    assert stages == [data_stage, stage0]


def test_downstream(M, tmp_dir, dvc):
    # The dependency graph should look like this:
    #
    #       E
    #      / \
    #     D   F
    #    / \   \
    #   B   C   G
    #    \ /
    #     A
    #
    assert main(["stage", "add", "--run", "-n", "A-gen", "-o", "A", "echo A>A"]) == 0
    assert (
        main(["stage", "add", "--run", "-n", "B-gen", "-d", "A", "-o", "B", "echo B>B"])
        == 0
    )
    assert (
        main(
            [
                "stage",
                "add",
                "--run",
                "-n",
                "C-gen",
                "-d",
                "A",
                "-o",
                "C",
                "echo C>C",
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "stage",
                "add",
                "--run",
                "-n",
                "D-gen",
                "-d",
                "B",
                "-d",
                "C",
                "-o",
                "D",
                "echo D>D",
            ]
        )
        == 0
    )
    assert main(["stage", "add", "--run", "-n", "G-gen", "-o", "G", "echo G>G"]) == 0
    assert (
        main(["stage", "add", "--run", "-n", "F-gen", "-d", "G", "-o", "F", "echo F>F"])
        == 0
    )
    assert (
        main(
            [
                "stage",
                "add",
                "--run",
                "-n",
                "E-gen",
                "-d",
                "D",
                "-d",
                "F",
                "-o",
                "E",
                "echo E>E",
            ]
        )
        == 0
    )

    # We want the evaluation to move from B to E
    #
    #       E
    #      /
    #     D
    #    /
    #   B
    #
    evaluation = dvc.reproduce(PROJECT_FILE + ":B-gen", downstream=True, force=True)

    assert len(evaluation) == 3
    assert all(isinstance(stage, PipelineStage) for stage in evaluation)
    assert all(stage.relpath == PROJECT_FILE for stage in evaluation)
    assert [stage.name for stage in evaluation] == ["B-gen", "D-gen", "E-gen"]

    # B, C should be run (in any order) before D
    # See https://github.com/iterative/dvc/issues/3602
    evaluation = dvc.reproduce(PROJECT_FILE + ":A-gen", downstream=True, force=True)

    assert len(evaluation) == 5
    assert all(isinstance(stage, PipelineStage) for stage in evaluation)
    assert all(stage.relpath == PROJECT_FILE for stage in evaluation)
    assert [stage.name for stage in evaluation] == [
        "A-gen",
        M.any_of("B-gen", "C-gen"),
        M.any_of("B-gen", "C-gen"),
        "D-gen",
        "E-gen",
    ]


def test_repro_when_cmd_changes(tmp_dir, dvc, run_copy, mocker):
    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")
    assert not dvc.reproduce(stage.addressing)

    from dvc.stage.run import cmd_run

    m = mocker.patch("dvc.stage.run.cmd_run", wraps=cmd_run)

    with modify_yaml("dvc.yaml") as d:
        # change cmd spacing by two
        d["stages"]["copy-foo-bar"]["cmd"] = "  ".join(stage.cmd.split())

    assert dvc.status([stage.addressing]) == {stage.addressing: ["changed command"]}
    assert dvc.reproduce(stage.addressing)[0] == stage
    m.assert_called_once_with(stage, dry=False, run_env=None)


def test_repro_when_new_deps_is_added_in_dvcfile(tmp_dir, dvc, run_copy, copy_script):
    from dvc.dvcfile import load_file

    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    stage = dvc.run(
        cmd="python copy.py {} {}".format("foo", "foobar"),
        outs=["foobar"],
        deps=["foo"],
        name="copy-file",
    )
    target = PROJECT_FILE + ":copy-file"
    assert not dvc.reproduce(target)

    dvcfile = load_file(dvc, stage.path)
    data, _ = dvcfile._load()
    data["stages"]["copy-file"]["deps"] += ["copy.py"]
    (tmp_dir / stage.path).dump(data)

    assert dvc.reproduce(target)[0] == stage


def test_repro_when_new_outs_is_added_in_dvcfile(tmp_dir, dvc, copy_script):
    from dvc.dvcfile import load_file

    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    stage = dvc.run(
        cmd="python copy.py {} {}".format("foo", "foobar"),
        outs=[],  # scenario where user forgot to add
        deps=["foo"],
        name="copy-file",
    )
    target = ":copy-file"
    assert not dvc.reproduce(target)

    dvcfile = load_file(dvc, stage.path)
    data, _ = dvcfile._load()
    data["stages"]["copy-file"]["outs"] = ["foobar"]
    (tmp_dir / stage.path).dump(data)

    assert dvc.reproduce(target)[0] == stage


def test_repro_when_new_deps_is_moved(tmp_dir, dvc, copy_script):
    from dvc.dvcfile import load_file

    tmp_dir.gen({"foo": "foo", "bar": "foo"})
    stage = dvc.run(
        cmd="python copy.py {} {}".format("foo", "foobar"),
        outs=["foobar"],
        deps=["foo"],
        name="copy-file",
    )
    target = ":copy-file"
    assert not dvc.reproduce(target)

    # hardcode values in source code, ignore sys.argv
    tmp_dir.gen(
        "copy.py",
        """
import shutil

shutil.copyfile('bar', 'foobar')
""",
    )
    from shutil import move

    move("foo", "bar")

    dvcfile = load_file(dvc, stage.path)
    data, _ = dvcfile._load()
    data["stages"]["copy-file"]["deps"] = ["bar"]
    (tmp_dir / stage.path).dump(data)

    assert dvc.reproduce(target)[0] == stage


def test_repro_when_new_out_overlaps_others_stage_outs(tmp_dir, dvc):
    from dvc.exceptions import OverlappingOutputPathsError

    tmp_dir.gen({"dir": {"file1": "file1"}, "foo": "foo"})
    dvc.add("dir")
    (tmp_dir / PROJECT_FILE).dump(
        {
            "stages": {
                "run-copy": {
                    "cmd": "python copy {} {}".format("foo", "dir/foo"),
                    "deps": ["foo"],
                    "outs": ["dir/foo"],
                }
            }
        },
    )
    with pytest.raises(OverlappingOutputPathsError):
        dvc.reproduce(":run-copy")


def test_repro_when_new_deps_added_does_not_exist(tmp_dir, dvc, copy_script):
    tmp_dir.gen("foo", "foo")
    (tmp_dir / PROJECT_FILE).dump(
        {
            "stages": {
                "run-copy": {
                    "cmd": "python copy.py {} {}".format("foo", "foobar"),
                    "deps": ["foo", "bar"],
                    "outs": ["foobar"],
                }
            }
        },
    )
    with pytest.raises(ReproductionError):
        dvc.reproduce(":run-copy")


def test_repro_when_new_outs_added_does_not_exist(tmp_dir, dvc, copy_script):
    tmp_dir.gen("foo", "foo")
    (tmp_dir / PROJECT_FILE).dump(
        {
            "stages": {
                "run-copy": {
                    "cmd": "python copy.py {} {}".format("foo", "foobar"),
                    "deps": ["foo"],
                    "outs": ["foobar", "bar"],
                }
            }
        },
    )
    with pytest.raises(ReproductionError):
        dvc.reproduce(":run-copy")


def test_repro_when_lockfile_gets_deleted(tmp_dir, dvc, copy_script):
    tmp_dir.gen("foo", "foo")
    (tmp_dir / PROJECT_FILE).dump(
        {
            "stages": {
                "run-copy": {
                    "cmd": "python copy.py {} {}".format("foo", "foobar"),
                    "deps": ["foo"],
                    "outs": ["foobar"],
                }
            }
        },
    )
    assert dvc.reproduce(":run-copy")
    assert os.path.exists(LOCK_FILE)

    assert not dvc.reproduce(":run-copy")
    os.unlink(LOCK_FILE)
    stages = dvc.reproduce(":run-copy")
    assert stages
    assert stages[0].relpath == PROJECT_FILE
    assert stages[0].name == "run-copy"


def test_cyclic_graph_error(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    run_copy("bar", "baz", name="copy-bar-baz")
    run_copy("baz", "foobar", name="copy-baz-foobar")

    with modify_yaml("dvc.yaml") as data:
        data["stages"]["copy-baz-foo"] = {
            "cmd": "echo baz > foo",
            "deps": ["baz"],
            "outs": ["foo"],
        }

    with pytest.raises(CyclicGraphError):
        dvc.reproduce(":copy-baz-foo")


def test_repro_multiple_params(tmp_dir, dvc):
    from dvc.stage.utils import split_params_deps
    from tests.func.test_run import supported_params

    (tmp_dir / "params2.yaml").dump(supported_params)
    (tmp_dir / "params.yaml").dump(supported_params)

    (tmp_dir / "foo").write_text("foo")
    stage = dvc.run(
        name="read_params",
        deps=["foo"],
        outs=["bar"],
        params=[
            "params2.yaml:lists,floats,name",
            "answer,floats,nested.nested1",
        ],
        cmd="cat params2.yaml params.yaml > bar",
    )

    params, deps = split_params_deps(stage)
    assert len(params) == 2
    assert len(deps) == 1
    assert len(stage.outs) == 1

    lockfile = stage.dvcfile._lockfile
    assert lockfile.load()["stages"]["read_params"]["params"] == {
        "params2.yaml": {
            "lists": [42, 42.0, "42"],
            "floats": 42.0,
            "name": "Answer",
        },
        "params.yaml": {
            "answer": 42,
            "floats": 42.0,
            "nested.nested1": {"nested2": "42", "nested2-2": 41.99999},
        },
    }
    data, _ = stage.dvcfile._load()
    params = data["stages"]["read_params"]["params"]

    custom, defaults = lsplit(lambda v: isinstance(v, dict), params)
    assert set(custom[0]["params2.yaml"]) == {"name", "lists", "floats"}
    assert set(defaults) == {"answer", "floats", "nested.nested1"}

    assert not dvc.reproduce(stage.addressing)
    params = deepcopy(supported_params)
    params["answer"] = 43
    (tmp_dir / "params.yaml").dump(params)

    assert dvc.reproduce(stage.addressing) == [stage]


@pytest.mark.parametrize("multiline", [True, False])
def test_repro_list_of_commands_in_order(tmp_dir, dvc, multiline):
    cmd = ["echo foo>foo", "echo bar>bar"]
    if multiline:
        cmd = "\n".join(cmd)

    (tmp_dir / "dvc.yaml").dump({"stages": {"multi": {"cmd": cmd}}})

    (tmp_dir / "dvc.yaml").write_text(
        dedent(
            """\
            stages:
              multi:
                cmd:
                - echo foo>foo
                - echo bar>bar
        """
        )
    )
    dvc.reproduce(targets=["multi"])
    assert (tmp_dir / "foo").read_text() == "foo\n"
    assert (tmp_dir / "bar").read_text() == "bar\n"


@pytest.mark.parametrize("multiline", [True, False])
def test_repro_list_of_commands_raise_and_stops_after_failure(tmp_dir, dvc, multiline):
    cmd = ["echo foo>foo", "failed_command", "echo baz>bar"]
    if multiline:
        cmd = "\n".join(cmd)

    (tmp_dir / "dvc.yaml").dump({"stages": {"multi": {"cmd": cmd}}})

    with pytest.raises(ReproductionError):
        dvc.reproduce(targets=["multi"])
    assert (tmp_dir / "foo").read_text() == "foo\n"
    assert not (tmp_dir / "bar").exists()


def test_repro_pulls_mising_data_source(tmp_dir, dvc, mocker, local_remote):
    (foo,) = tmp_dir.dvc_gen("foo", "foo")

    dvc.push()

    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    remove("foo")
    remove(foo.outs[0].cache_path)

    assert dvc.reproduce(pull=True)


def test_repro_pulls_mising_import(tmp_dir, dvc, mocker, erepo_dir, local_remote):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo", commit="first")

    foo_import = dvc.imp(os.fspath(erepo_dir), "foo")

    dvc.push()

    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    remove("foo")
    remove(foo_import.outs[0].cache_path)

    assert dvc.reproduce(pull=True)


def test_repro_allow_missing(tmp_dir, dvc):
    tmp_dir.gen("fixed", "fixed")
    dvc.stage.add(name="create-foo", cmd="echo foo > foo", deps=["fixed"], outs=["foo"])
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    (create_foo, copy_foo) = dvc.reproduce()

    remove("foo")
    remove(create_foo.outs[0].cache_path)
    remove(dvc.stage_cache.cache_dir)

    ret = dvc.reproduce(allow_missing=True)
    # both stages are skipped
    assert not ret


def test_repro_allow_missing_and_pull(tmp_dir, dvc, mocker, local_remote):
    tmp_dir.gen("fixed", "fixed")
    dvc.stage.add(name="create-foo", cmd="echo foo > foo", deps=["fixed"], outs=["foo"])
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    (create_foo,) = dvc.reproduce("create-foo")

    dvc.push()

    remove("foo")
    remove(create_foo.outs[0].cache_path)
    remove(dvc.stage_cache.cache_dir)

    ret = dvc.reproduce(pull=True, allow_missing=True)
    # create-foo is skipped ; copy-foo pulls missing dep
    assert len(ret) == 1


def test_repro_pulls_continue_without_run_cache(tmp_dir, dvc, mocker, local_remote):
    (foo,) = tmp_dir.dvc_gen("foo", "foo")

    dvc.push()
    mocker.patch.object(
        dvc.stage_cache, "pull", side_effect=RunCacheNotSupported("foo")
    )
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    remove("foo")
    remove(foo.outs[0].cache_path)

    assert dvc.reproduce(pull=True)


def test_repro_skip_pull_if_no_run_cache_is_passed(tmp_dir, dvc, mocker, local_remote):
    (foo,) = tmp_dir.dvc_gen("foo", "foo")

    dvc.push()
    spy_pull = mocker.spy(dvc.stage_cache, "pull")
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    remove("foo")
    remove(foo.outs[0].cache_path)

    assert dvc.reproduce(pull=True, run_cache=False)
    assert not spy_pull.called


def test_repro_no_commit(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    remove(dvc.cache.local.path)
    ret = main(["repro", stage.addressing, "--no-commit"])
    assert ret == 0
    # run-cache should be skipped if `-no-commit`.
    assert not os.path.isdir(dvc.cache.local.path)


def test_repro_all_pipelines(
    mocker,
    dvc,
):
    stages = [
        dvc.run(
            outs=["start.txt"],
            cmd="echo start > start.txt",
            name="start",
        ),
        dvc.run(
            deps=["start.txt"],
            outs=["middle.txt"],
            cmd="echo middle > middle.txt",
            name="middle",
        ),
        dvc.run(
            deps=["middle.txt"],
            outs=["final.txt"],
            cmd="echo final > final.txt",
            name="final",
        ),
        dvc.run(
            outs=["disconnected.txt"],
            cmd="echo other > disconnected.txt",
            name="disconnected",
        ),
    ]

    from dvc_data.hashfile.state import StateNoop

    dvc.state = StateNoop()

    mock_reproduce = mocker.patch.object(Stage, "reproduce", side_effect=stages)
    ret = main(["repro", "--all-pipelines"])
    assert ret == 0
    assert mock_reproduce.call_count == 4


class TestReproAlreadyCached:
    def test(
        self,
        dvc,
    ):
        stage = dvc.run(
            always_changed=True,
            deps=[],
            outs=["datetime.txt"],
            cmd='python -c "import time; print(time.time())" > datetime.txt',
            name="datetime",
        )
        run_out = stage.outs[0]
        repro_out = dvc.reproduce(stage.addressing)[0].outs[0]

        assert run_out.hash_info != repro_out.hash_info

    def test_force_with_dependencies(
        self,
        tmp_dir,
        dvc,
    ):
        tmp_dir.dvc_gen("foo", "foo")
        stage = dvc.run(
            name="datetime",
            deps=["foo"],
            outs=["datetime.txt"],
            cmd='python -c "import time; print(time.time())" > datetime.txt',
        )

        ret = main(["repro", "--force", stage.addressing])
        assert ret == 0

        saved_stage = dvc.stage.get_target(stage.addressing)
        assert stage.outs[0].hash_info != saved_stage.outs[0].hash_info

    def test_force_import(self, mocker, tmp_dir, dvc):
        from dvc.dependency import base

        tmp_dir.dvc_gen("foo", "foo")

        ret = main(["import-url", "foo", "bar"])
        assert ret == 0

        spy_get = mocker.spy(base, "fs_download")
        spy_checkout = mocker.spy(Output, "checkout")

        assert main(["unfreeze", "bar.dvc"]) == 0
        ret = main(["repro", "--force", "bar.dvc"])
        assert ret == 0
        assert spy_get.call_count == 1
        assert spy_checkout.call_count == 0


@pytest.mark.skipif(os.name == "nt", reason="not on nt")
def test_repro_shell(tmp_dir, monkeypatch, dvc):
    monkeypatch.setenv("SHELL", "/bin/sh")
    dvc.run(outs=["shell.txt"], cmd="echo $SHELL > shell.txt", name="echo-shell")
    shell = os.getenv("SHELL")

    assert (tmp_dir / "shell.txt").read_text().rstrip() == shell
    (tmp_dir / "shell.txt").unlink()

    dvc.reproduce("echo-shell")
    assert (tmp_dir / "shell.txt").read_text().rstrip() == shell


def test_cmd_repro(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    shutil.copyfile("bar", "foo")

    ret = main(["status"])
    assert ret == 0

    ret = main(["repro", stage.addressing])
    assert ret == 0

    ret = main(["repro", "non-existing-file"])
    assert ret != 0


def test_repro_dep_under_dir(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("foo", "foo")
    tmp_dir.dvc_gen("data", {"file": "file", "sub": {"foo": "foo"}})

    stage = dvc.run(
        outs=["file1"],
        deps=["data/file", "copy.py"],
        cmd="python copy.py data/file file1",
        name="copy-data-file1",
    )

    assert filecmp.cmp("file1", "data/file", shallow=False)

    os.unlink("data/file")
    shutil.copyfile("foo", "data/file")

    stages = dvc.reproduce(stage.addressing)
    assert len(stages) == 2
    assert filecmp.cmp("file1", "foo", shallow=False)


def test_repro_dep_dir_with_outputs_under_it(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("foo", "foo")
    file_stage, _ = tmp_dir.dvc_gen(
        {"data/file": "file", "data/sub": {"foo": "foo", "bar": "bar"}}
    )
    dvc.run(
        cmd="ls data/file data/sub",
        deps=["data/file", "data/sub"],
        name="list-files",
    )
    copy_stage = dvc.run(
        deps=["data"],
        outs=["file1"],
        cmd="python copy.py data file1",
        name="copy-data-file1",
    )
    os.unlink("data/file")
    shutil.copyfile("foo", "data/file")
    assert dvc.reproduce(copy_stage.addressing) == [file_stage, copy_stage]


def test_repro_force(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    stages = dvc.reproduce(stage.addressing, force=True)
    assert len(stages) == 2


def test_repro_changed_code(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    with (tmp_dir / "copy.py").open("a+", encoding="utf8") as f:
        f.write("\nshutil.copyfile('bar', sys.argv[2])")
    stages = dvc.reproduce(stage.addressing)

    assert filecmp.cmp("file1", "bar", shallow=False)
    assert len(stages) == 1


def test_repro_changed_data(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    shutil.copyfile("bar", "foo")

    stages = dvc.reproduce(stage.addressing)

    assert filecmp.cmp("file1", "bar", shallow=False)
    assert len(stages) == 2


def test_repro_dry(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    shutil.copyfile("bar", "foo")

    stages = dvc.reproduce(stage.addressing, dry=True)

    assert len(stages), 2
    assert not filecmp.cmp("file1", "bar", shallow=False)

    ret = main(["repro", "--dry", stage.addressing])
    assert ret == 0
    assert not filecmp.cmp("file1", "bar", shallow=False)


def test_repro_up_to_date(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    ret = main(["repro", stage.addressing])
    assert ret == 0


def test_repro_dry_no_exec(tmp_dir, dvc):
    deps = []
    for d in range(3):
        idir = f"idir{d}"
        odir = f"odir{d}"

        deps.append("-d")
        deps.append(odir)

        os.mkdir(idir)

        f = os.path.join(idir, "file")
        with open(f, "w+", encoding="utf-8") as fobj:
            fobj.write(str(d))

        ret = main(
            [
                "stage",
                "add",
                "-n",
                f"copy-{idir}-{odir}",
                "-d",
                idir,
                "-o",
                odir,
                f'python -c \'import shutil; shutil.copytree("{idir}", "{odir}")\'',
            ]
        )
        assert ret == 0

    ret = main(
        [
            "stage",
            "add",
            "-n",
            "ls",
            *deps,
            "ls {}".format(" ".join(dep for i, dep in enumerate(deps) if i % 2)),
        ]
    )
    assert ret == 0

    ret = main(["repro", "--dry", "ls"])
    assert ret == 0


def test_repro_changed_deep_data(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    file2_stage = dvc.run(
        outs=["file2"],
        deps=["file1", "copy.py"],
        cmd="python copy.py file1 file2",
        name="copy-file-file2",
    )
    shutil.copyfile("bar", "foo")
    stages = dvc.reproduce(file2_stage.addressing)
    assert filecmp.cmp("file1", "bar", shallow=False)
    assert filecmp.cmp("file2", "bar", shallow=False)
    assert len(stages) == 3


def test_repro_force_downstream(tmp_dir, dvc, copy_script):
    tmp_dir.gen("foo", "foo")
    stages = dvc.add("foo")
    assert len(stages) == 1
    foo_stage = stages[0]
    assert foo_stage is not None

    shutil.copyfile("copy.py", "copy1.py")
    file1 = "file1"
    file1_stage = dvc.run(
        outs=[file1],
        deps=["foo", "copy1.py"],
        cmd=f"python copy1.py foo {file1}",
        name="copy-foo-file1",
    )
    assert file1_stage is not None

    shutil.copyfile("copy.py", "copy2.py")
    file2 = "file2"
    file2_stage = dvc.run(
        outs=[file2],
        deps=[file1, "copy2.py"],
        cmd=f"python copy2.py {file1} {file2}",
        name="copy-file1-file2",
    )
    assert file2_stage is not None

    shutil.copyfile("copy.py", "copy3.py")
    file3 = "file3"
    file3_stage = dvc.run(
        outs=[file3],
        deps=[file2, "copy3.py"],
        cmd=f"python copy3.py {file2} {file3}",
        name="copy-file2-file3",
    )
    assert file3_stage is not None

    with open("copy2.py", "a", encoding="utf-8") as fobj:
        fobj.write("\n\n")

    stages = dvc.reproduce(file3_stage.addressing, force_downstream=True)
    assert len(stages) == 2
    assert stages[0].addressing == file2_stage.addressing
    assert stages[1].addressing == file3_stage.addressing


def test_repro_force_downstream_do_not_force_independent_stages(tmp_dir, dvc, run_copy):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    foo1 = run_copy("foo", "foo1", name="foo1")
    foo2 = run_copy("foo1", "foo2", name="foo2")
    run_copy("bar", "bar1", name="bar1")
    run_copy("bar1", "bar2", name="bar2")
    cat = dvc.run(cmd="cat bar2 foo2", deps=["foo2", "bar2"], name="cat")

    tmp_dir.gen("foo", "foobar")
    assert dvc.reproduce(force_downstream=True) == [foo1, foo2, cat]


def test_repro_pipeline(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    stage = dvc.run(
        outs=["file2"],
        deps=["file1", "copy.py"],
        cmd="python copy.py file1 file2",
        name="copy-file-file2",
    )
    stages = dvc.reproduce(stage.addressing, force=True, pipeline=True)
    assert len(stages) == 3


def test_repro_pipeline_cli(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    ret = main(["repro", "--pipeline", "-f", stage.addressing])
    assert ret == 0


def test_repro_pipelines(
    tmp_dir,
    dvc,
    copy_script,
):
    foo_stage, bar_stage = tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})
    file1_stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="copy-FOO-file1",
    )
    file2_stage = dvc.run(
        outs=["file2"],
        deps=["bar", "copy.py"],
        cmd="python copy.py bar file2",
        name="copy-BAR-file2",
    )
    assert set(dvc.reproduce(all_pipelines=True, force=True)) == {
        foo_stage,
        bar_stage,
        file1_stage,
        file2_stage,
    }


def test_repro_pipelines_cli(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})
    dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="copy-FOO-file1",
    )
    dvc.run(
        outs=["file2"],
        deps=["bar", "copy.py"],
        cmd="python copy.py bar file2",
        name="copy-BAR-file2",
    )
    assert main(["repro", "-f", "-P"]) == 0


@pytest.mark.parametrize(
    "target",
    [
        "Dvcfile",
        "pipelines.yaml",
        "pipelines.yaml:name",
        "Dvcfile:name",
        "stage.dvc",
        "stage.dvc:name",
        "not-existing-stage.json",
    ],
)
def test_freeze_non_existing(dvc, target):
    with pytest.raises(StageFileDoesNotExistError):
        dvc.freeze(target)

    ret = main(["freeze", target])
    assert ret != 0


def test_repro_frozen_callback(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("foo", "foo")
    # NOTE: purposefully not specifying deps or outs
    # to create a callback stage.
    stage = dvc.run(
        cmd="python copy.py foo file1",
        name="copy-FOO-file1",
    )

    stages = dvc.reproduce(stage.addressing)
    assert len(stages) == 1

    dvc.freeze(stage.addressing)
    stages = dvc.reproduce(stage.addressing)
    assert len(stages) == 0

    dvc.unfreeze(stage.addressing)
    stages = dvc.reproduce(stage.addressing)
    assert len(stages) == 1


def test_repro_frozen_unchanged(
    tmp_dir,
    dvc,
    copy_script,
):
    """
    Check that freezing/unfreezing doesn't affect stage state
    """
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    target = stage.addressing
    dvc.freeze(target)
    stages = dvc.reproduce(target)
    assert len(stages) == 0

    dvc.unfreeze(target)
    stages = dvc.reproduce(target)
    assert len(stages) == 0


def test_repro_metrics_add_unchanged(tmp_dir, dvc, copy_script):
    """
    Check that adding/removing metrics doesn't affect stage state
    """
    tmp_dir.gen("foo", "foo")
    stages = dvc.add("foo")
    assert len(stages) == 1
    assert stages[0] is not None

    dvc.run(
        outs_no_cache=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="copy",
    )

    stages = dvc.reproduce("copy")
    assert len(stages) == 0

    dvc.stage.add(
        metrics_no_cache=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="copy",
        force=True,
    )

    stages = dvc.reproduce("copy")
    assert len(stages) == 0

    dvc.stage.add(
        outs_no_cache=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="copy",
        force=True,
    )

    stages = dvc.reproduce("copy")
    assert len(stages) == 0


def test_repro_phony(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    stage = dvc.run(cmd="cat file1", deps=["file1"], name="cat")
    shutil.copyfile("bar", "foo")

    dvc.reproduce(stage.addressing)

    assert filecmp.cmp("file1", "bar", shallow=False)


def test_non_existing_output(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    os.unlink("foo")

    with pytest.raises(ReproductionError):
        dvc.reproduce(stage.addressing)


def test_repro_data_source(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    shutil.copyfile("bar", "foo")

    stages = dvc.reproduce(stage.addressing)

    assert filecmp.cmp("foo", "bar", shallow=False)
    assert stages[0].outs[0].hash_info.value == file_md5("bar")


def test_repro_changed_dir(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    shutil.copyfile("foo", "file")

    stage = dvc.run(
        outs=["dir"],
        deps=["file", "copy.py"],
        cmd="mkdir dir && python copy.py foo dir/foo",
        name="copy-in-dir",
    )

    stages = dvc.reproduce(stage.addressing)
    assert len(stages) == 0

    os.unlink("file")
    shutil.copyfile("bar", "file")

    stages = dvc.reproduce(stage.addressing)
    assert len(stages) == 1


def test_repro_changed_dir_data(
    tmp_dir,
    dvc,
    copy_script,
):
    tmp_dir.gen({"data": {"foo": "foo"}, "bar": "bar"})
    stage = dvc.run(
        outs=["dir"],
        deps=["data", "copy.py"],
        cmd="python copy.py data dir",
        name="copy-dir",
    )

    assert not dvc.reproduce(stage.addressing)

    with (tmp_dir / "data" / "foo").open("a", encoding="utf-8") as f:
        f.write("add")

    stages = dvc.reproduce(stage.addressing)
    assert len(stages) == 1

    # Check that dvc indeed registers changed output dir
    shutil.move("bar", "dir")
    stages = dvc.reproduce(stage.addressing)
    assert len(stages) == 1

    file = os.path.join("data", "foo")
    # Check that dvc registers mtime change for the directory.
    system.hardlink(file, file + ".lnk")
    stages = dvc.reproduce(stage.addressing)
    assert len(stages) == 1


def test_repro_missing_lock_info(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.stage.add(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="copy-foo-file1",
    )

    stages = dvc.reproduce(stage.addressing)
    assert len(stages) == 1


def test_repro_rm_recursive(tmp_dir, dvc):
    # check that dir output recursively removes files in the dir
    tmp_dir.gen({"dir": {"foo": "foo"}})
    dvc.stage.add(name="dir", cmd="mkdir dir", outs=["dir"])
    dvc.reproduce()
    assert (tmp_dir / "dir").exists()
    assert not (tmp_dir / "dir" / "foo").exists()


def test_repro_single_item_with_multiple_targets(tmp_dir, dvc, copy_script):
    stage1 = dvc.stage.add(cmd="echo foo > foo", outs=["foo"], name="gen-foo")
    with dvc.lock:
        stage1.run()

    stage2 = dvc.stage.add(
        cmd="python copy.py foo bar", deps=["foo"], outs=["bar"], name="copy-foo-bar"
    )
    assert dvc.reproduce(["copy-foo-bar", "gen-foo"], single_item=True) == [
        stage2,
        stage1,
    ]


def test_repro_keep_going(mocker, tmp_dir, dvc, copy_script):
    from dvc.repo import reproduce

    (bar_stage, foo_stage) = tmp_dir.dvc_gen({"bar": "bar", "foo": "foo"})
    stage1 = dvc.stage.add(
        cmd=["python copy.py bar foobar", "exit 1"],
        deps=["bar"],
        outs=["foobar"],
        name="copy-bar-foobar",
    )
    dvc.stage.add(cmd="cat foobar foo", deps=["foobar", "foo"], name="cat")
    spy = mocker.spy(reproduce, "_reproduce_stage")

    with pytest.raises(ReproductionError):
        dvc.reproduce(on_error="keep-going", repro_fn=spy)

    assert spy.call_args_list == [
        mocker.call(bar_stage, upstream=[], force=False, interactive=False),
        mocker.call(stage1, upstream=[bar_stage], force=False, interactive=False),
        mocker.call(foo_stage, upstream=[], force=False, interactive=False),
    ]


def test_repro_ignore_errors(M, mocker, tmp_dir, dvc, copy_script):
    from dvc.repo import reproduce

    (bar_stage, foo_stage) = tmp_dir.dvc_gen({"bar": "bar", "foo": "foo"})
    stage1 = dvc.stage.add(
        cmd=["python copy.py bar foobar", "exit 1"],
        deps=["bar"],
        outs=["foobar"],
        name="copy-bar-foobar",
    )
    stage2 = dvc.stage.add(cmd="cat foobar foo", deps=["foobar", "foo"], name="cat")
    spy = mocker.spy(reproduce, "_reproduce_stage")
    dvc.reproduce(on_error="ignore", repro_fn=spy)

    assert spy.call_args_list == [
        mocker.call(bar_stage, upstream=[], force=False, interactive=False),
        mocker.call(stage1, upstream=[bar_stage], force=False, interactive=False),
        mocker.call(foo_stage, upstream=[], force=False, interactive=False),
        mocker.call(
            stage2,
            upstream=[foo_stage, stage1],
            force=False,
            interactive=False,
        ),
    ]
