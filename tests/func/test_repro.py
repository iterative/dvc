import filecmp
import os
import re
import shutil
from pathlib import Path

import pytest

from dvc.cli import main
from dvc.dvcfile import DVC_FILE, load_file
from dvc.exceptions import CyclicGraphError, ReproductionError, StagePathAsOutputError
from dvc.fs import LocalFileSystem, system
from dvc.output import Output
from dvc.stage import Stage
from dvc.stage.exceptions import StageFileDoesNotExistError
from dvc.utils import relpath
from dvc.utils.fs import remove
from dvc.utils.serialize import dump_yaml, load_yaml
from dvc_data.hashfile.hash import file_md5


@pytest.fixture(
    params=(
        {"single_stage": True},
        {"single_stage": False},
    ),
    ids=["single_stage", "multi_stage"],
)
def run_stage(dvc, request):
    def inner(*args, name=None, **kwargs):
        assert name
        # these should not be passed
        assert "single_stage" not in kwargs
        assert "fname" not in kwargs

        if request.param["single_stage"]:
            kwargs.update(
                {
                    "fname": name + ".dvc",
                    "single_stage": True,
                }
            )
        else:
            kwargs["name"] = name
        return dvc.run(*args, **kwargs)

    return inner


def test_repro_fail(tmp_dir, run_stage, copy_script):
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_stage(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    os.unlink("copy.py")
    assert main(["repro", stage.addressing]) != 0


def test_repro_cyclic_graph(tmp_dir, dvc, run_stage):
    tmp_dir.gen("foo", "foo")
    run_stage(
        deps=["foo"],
        outs=["bar.txt"],
        cmd="echo bar > bar.txt",
        name="copybarbar-txt",
    )
    run_stage(
        deps=["bar.txt"],
        outs=["baz.txt"],
        cmd="echo baz > baz.txt",
        name="copybazbaz-txt",
    )

    stage_dump = {
        "cmd": "echo baz > foo",
        "deps": [{"path": "baz.txt"}],
        "outs": [{"path": "foo"}],
    }
    dump_yaml("cycle.dvc", stage_dump)

    with pytest.raises(CyclicGraphError):
        dvc.reproduce("cycle.dvc")


class TestReproWorkingDirectoryAsOutput:
    """
    |  stage.cwd  |  out.path | cwd as output |
    |:-----------:|:---------:|:-------------:|
    |     dir     |    dir    |      True     |
    | dir/subdir/ |    dir    |      True     |
    |     dir     |   dir-1   |     False     |
    |      .      | something |     False     |
    """

    def test(self, dvc):
        # File structure:
        #       .
        #       |-- dir1
        #       |  |__ dir2.dvc         (out.path == ../dir2)
        #       |__ dir2
        #           |__ something.dvc    (stage.cwd == ./dir2)

        os.mkdir(os.path.join(dvc.root_dir, "dir1"))

        dvc.run(
            fname=os.path.join("dir1", "dir2.dvc"),
            wdir="dir1",
            outs=[os.path.join("..", "dir2")],
            cmd="mkdir {path}".format(path=os.path.join("..", "dir2")),
            single_stage=True,
        )

        faulty_stage_path = os.path.join("dir2", "something.dvc")

        output = os.path.join("..", "something")
        stage_dump = {
            "cmd": f"echo something > {output}",
            "outs": [{"path": output}],
        }
        dump_yaml(faulty_stage_path, stage_dump)

        with pytest.raises(StagePathAsOutputError):
            dvc.reproduce(faulty_stage_path)

    def test_nested(self, mocker, dvc):
        #       .
        #       |-- a
        #       |  |__ nested
        #       |     |__ dir
        #       |       |__ error.dvc     (stage.cwd == 'a/nested/dir')
        #       |__ b
        #          |__ nested.dvc         (stage.out == 'a/nested')
        dir1 = "b"
        dir2 = "a"

        os.mkdir(dir1)
        os.mkdir(dir2)

        nested_dir = os.path.join(dir2, "nested")
        out_dir = relpath(nested_dir, dir1)

        nested_stage = dvc.run(
            fname=os.path.join(dir1, "b.dvc"),
            wdir=dir1,
            outs=[out_dir],  # ../a/nested
            cmd=f"mkdir {out_dir}",
            single_stage=True,
        )

        os.mkdir(os.path.join(nested_dir, "dir"))

        error_stage_path = os.path.join(nested_dir, "dir", "error.dvc")

        output = os.path.join("..", "..", "something")
        stage_dump = {
            "cmd": f"echo something > {output}",
            "outs": [{"path": output}],
        }
        dump_yaml(error_stage_path, stage_dump)

        # NOTE: os.walk() walks in a sorted order and we need dir2 subdirs to
        # be processed before dir1 to load error.dvc first.
        dvc.index = dvc.index.update(
            [
                nested_stage,
                load_file(dvc, error_stage_path).stage,
            ]
        )

        mocker.patch.object(dvc, "_reset")  # to prevent `stages` resetting
        with pytest.raises(StagePathAsOutputError):
            dvc.reproduce(error_stage_path)

    def test_similar_paths(self, dvc):
        # File structure:
        #
        #       .
        #       |-- something.dvc   (out.path == something)
        #       |-- something
        #       |__ something-1
        #          |-- a
        #          |__ a.dvc        (stage.cwd == something-1)

        dvc.run(outs=["something"], cmd="mkdir something", single_stage=True)

        os.mkdir("something-1")

        stage = os.path.join("something-1", "a.dvc")

        stage_dump = {"cmd": "echo a > a", "outs": [{"path": "a"}]}
        dump_yaml(stage, stage_dump)

        dvc.reproduce(stage)


def test_repro_dep_under_dir(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("foo", "foo")
    tmp_dir.dvc_gen("data", {"file": "file", "sub": {"foo": "foo"}})

    stage = run_stage(
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


def test_repro_dep_dir_with_outputs_under_it(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("foo", "foo")
    file_stage, _ = tmp_dir.dvc_gen(
        {"data/file": "file", "data/sub": {"foo": "foo", "bar": "bar"}}
    )
    run_stage(
        cmd="ls data/file data/sub",
        deps=["data/file", "data/sub"],
        name="list-files",
    )
    copy_stage = run_stage(
        deps=["data"],
        outs=["file1"],
        cmd="python copy.py data file1",
        name="copy-data-file1",
    )
    os.unlink("data/file")
    shutil.copyfile("foo", "data/file")
    assert dvc.reproduce(copy_stage.addressing) == [file_stage, copy_stage]


def test_repro_force(tmp_dir, dvc, run_stage, copy_script):
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_stage(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    stages = dvc.reproduce(stage.addressing, force=True)
    assert len(stages) == 2


def test_repro_changed_code(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_stage(
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


def test_repro_changed_data(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_stage(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    shutil.copyfile("bar", "foo")

    stages = dvc.reproduce(stage.addressing)

    assert filecmp.cmp("file1", "bar", shallow=False)
    assert len(stages) == 2


def test_repro_dry(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_stage(
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


def test_repro_up_to_date(tmp_dir, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_stage(
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
                "run",
                "--no-exec",
                "--single-stage",
                "-d",
                idir,
                "-o",
                odir,
                'python -c \'import shutil; shutil.copytree("{}", "{}")\''.format(
                    idir, odir
                ),
            ]
        )
        assert ret == 0

    ret = main(
        [
            "run",
            "--no-exec",
            "--single-stage",
            "--file",
            DVC_FILE,
            *deps,
            "ls {}".format(" ".join(dep for i, dep in enumerate(deps) if i % 2)),
        ]
    )
    assert ret == 0

    ret = main(["repro", "--dry", DVC_FILE])
    assert ret == 0


def test_repro_changed_deep_data(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    run_stage(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    file2_stage = run_stage(
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
        single_stage=True,
    )
    assert file1_stage is not None

    shutil.copyfile("copy.py", "copy2.py")
    file2 = "file2"
    file2_stage = dvc.run(
        outs=[file2],
        deps=[file1, "copy2.py"],
        cmd=f"python copy2.py {file1} {file2}",
        single_stage=True,
    )
    assert file2_stage is not None

    shutil.copyfile("copy.py", "copy3.py")
    file3 = "file3"
    file3_stage = dvc.run(
        outs=[file3],
        deps=[file2, "copy3.py"],
        cmd=f"python copy3.py {file2} {file3}",
        single_stage=True,
    )
    assert file3_stage is not None

    with open("copy2.py", "a", encoding="utf-8") as fobj:
        fobj.write("\n\n")

    stages = dvc.reproduce(file3_stage.path, force_downstream=True)
    assert len(stages) == 2
    assert stages[0].path == file2_stage.path
    assert stages[1].path == file3_stage.path


def test_repro_pipeline(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    run_stage(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    stage = run_stage(
        outs=["file2"],
        deps=["file1", "copy.py"],
        cmd="python copy.py file1 file2",
        name="copy-file-file2",
    )
    stages = dvc.reproduce(stage.addressing, force=True, pipeline=True)
    assert len(stages) == 3


def test_repro_pipeline_cli(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_stage(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    ret = main(["repro", "--pipeline", "-f", stage.addressing])
    assert ret == 0


def test_repro_pipelines(tmp_dir, dvc, copy_script, run_stage):
    foo_stage, bar_stage = tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})
    file1_stage = run_stage(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="copy-FOO-file1",
    )
    file2_stage = run_stage(
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


def test_repro_pipelines_cli(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})
    run_stage(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="copy-FOO-file1",
    )
    run_stage(
        outs=["file2"],
        deps=["bar", "copy.py"],
        cmd="python copy.py bar file2",
        name="copy-BAR-file2",
    )
    assert main(["repro", "-f", "-P"]) == 0


def test_repro_frozen(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    run_stage(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    file2_stage = run_stage(
        outs=["file2"],
        deps=["file1", "copy.py"],
        cmd="python copy.py file1 file2",
        name="copy-file1-file2",
    )

    shutil.copyfile("bar", "foo")

    ret = main(["freeze", file2_stage.addressing])
    assert ret == 0
    stages = dvc.reproduce(file2_stage.addressing)
    assert len(stages) == 0

    ret = main(["unfreeze", file2_stage.addressing])
    assert ret == 0
    stages = dvc.reproduce(file2_stage.addressing)
    assert filecmp.cmp("file1", "bar", shallow=False)
    assert filecmp.cmp("file2", "bar", shallow=False)
    assert len(stages) == 3


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


def test_repro_frozen_callback(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("foo", "foo")
    # NOTE: purposefully not specifying deps or outs
    # to create a callback stage.
    stage = run_stage(
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


def test_repro_frozen_unchanged(tmp_dir, dvc, copy_script, run_stage):
    """
    Check that freezing/unfreezing doesn't affect stage state
    """
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_stage(
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

    file1 = "file1"
    file1_stage = file1 + ".dvc"
    dvc.run(
        fname=file1_stage,
        outs_no_cache=[file1],
        deps=["foo", "copy.py"],
        cmd=f"python copy.py foo {file1}",
        single_stage=True,
    )

    stages = dvc.reproduce(file1_stage)
    assert len(stages) == 0

    d = load_yaml(file1_stage)
    d["outs"][0]["metric"] = True
    dump_yaml(file1_stage, d)

    stages = dvc.reproduce(file1_stage)
    assert len(stages) == 0

    d = load_yaml(file1_stage)
    d["outs"][0]["metric"] = False
    dump_yaml(file1_stage, d)

    stages = dvc.reproduce(file1_stage)
    assert len(stages) == 0


def test_repro_phony(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    run_stage(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    stage = run_stage(cmd="cat file1", deps=["file1"], name="cat")
    shutil.copyfile("bar", "foo")

    dvc.reproduce(stage.addressing)

    assert filecmp.cmp("file1", "bar", shallow=False)


def test_non_existing_output(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_stage(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    os.unlink("foo")

    with pytest.raises(ReproductionError):
        dvc.reproduce(stage.addressing)


def test_repro_data_source(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_stage(
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        name="run1",
    )
    shutil.copyfile("bar", "foo")

    stages = dvc.reproduce(stage.addressing)

    assert filecmp.cmp("foo", "bar", shallow=False)
    assert stages[0].outs[0].hash_info.value == file_md5("bar")


def test_repro_changed_dir(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    shutil.copyfile("foo", "file")

    stage = run_stage(
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


def test_repro_changed_dir_data(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen({"data": {"foo": "foo"}, "bar": "bar"})
    stage = run_stage(
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


def test_repro_missing_md5_in_stage_file(tmp_dir, dvc, copy_script):
    tmp_dir.dvc_gen("foo", "foo")
    stage = dvc.run(
        fname="file1.dvc",
        outs=["file1"],
        deps=["foo", "copy.py"],
        cmd="python copy.py foo file1",
        single_stage=True,
    )
    d = load_yaml(stage.relpath)
    del d[Stage.PARAM_OUTS][0][LocalFileSystem.PARAM_CHECKSUM]
    del d[Stage.PARAM_DEPS][0][LocalFileSystem.PARAM_CHECKSUM]
    dump_yaml(stage.relpath, d)

    stages = dvc.reproduce(stage.addressing)
    assert len(stages) == 1


def test_cmd_repro(tmp_dir, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_stage(
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


@pytest.mark.skipif(os.name == "nt", reason="not on nt")
def test_repro_shell(tmp_dir, monkeypatch, dvc):
    monkeypatch.setenv("SHELL", "/bin/sh")
    dvc.run(
        fname="shell.txt.dvc",
        outs=["shell.txt"],
        cmd="echo $SHELL > shell.txt",
        single_stage=True,
    )
    shell = os.getenv("SHELL")

    assert (tmp_dir / "shell.txt").read_text().rstrip() == shell
    (tmp_dir / "shell.txt").unlink()

    dvc.reproduce("shell.txt.dvc")
    assert (tmp_dir / "shell.txt").read_text().rstrip() == shell


def test_repro_all_pipelines(mocker, dvc, run_stage):
    stages = [
        run_stage(
            outs=["start.txt"],
            cmd="echo start > start.txt",
            name="start",
        ),
        run_stage(
            deps=["start.txt"],
            outs=["middle.txt"],
            cmd="echo middle > middle.txt",
            name="middle",
        ),
        run_stage(
            deps=["middle.txt"],
            outs=["final.txt"],
            cmd="echo final > final.txt",
            name="final",
        ),
        run_stage(
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


def test_repro_no_commit(tmp_dir, dvc, copy_script, run_stage):
    tmp_dir.gen("bar", "bar")
    tmp_dir.dvc_gen("foo", "foo")
    stage = run_stage(
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


class TestReproAlreadyCached:
    def test(self, dvc, run_stage):
        stage = run_stage(
            always_changed=True,
            deps=[],
            outs=["datetime.txt"],
            cmd='python -c "import time; print(time.time())" > datetime.txt',
            name="datetime",
        )
        run_out = stage.outs[0]
        repro_out = dvc.reproduce(stage.addressing)[0].outs[0]

        assert run_out.hash_info != repro_out.hash_info

    def test_force_with_dependencies(self, tmp_dir, dvc, run_stage):
        tmp_dir.dvc_gen("foo", "foo")
        stage = run_stage(
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
        tmp_dir.dvc_gen("foo", "foo")

        ret = main(["import-url", "foo", "bar"])
        assert ret == 0

        spy_get = mocker.spy(LocalFileSystem, "get")
        spy_checkout = mocker.spy(Output, "checkout")

        assert main(["unfreeze", "bar.dvc"]) == 0
        ret = main(["repro", "--force", "bar.dvc"])
        assert ret == 0
        assert spy_get.call_count == 1
        assert spy_checkout.call_count == 0


def test_should_display_metrics_on_repro_with_metrics_option(caplog, capsys, dvc):
    metrics_file = "metrics_file"
    metrics_value = 0.123489015
    ret = main(
        [
            "run",
            "--single-stage",
            "-m",
            metrics_file,
            f"echo {metrics_value} >> {metrics_file}",
        ]
    )
    assert ret == 0

    caplog.clear()
    capsys.readouterr()  # clearing the buffer

    from dvc.dvcfile import DVC_FILE_SUFFIX

    ret = main(["repro", "--force", "--metrics", metrics_file + DVC_FILE_SUFFIX])
    assert ret == 0

    expected_metrics_display = f"Path\n{metrics_file}  {metrics_value}\n"
    actual, _ = capsys.readouterr()
    assert expected_metrics_display in actual


@pytest.fixture
def repro_dir(tmp_dir, dvc, run_copy):
    # Creates repo with following structure:
    #    data_dir/dir_file              origin_data
    #         |       |                   |
    #         |       |              origin_copy.dvc
    # unrelated2.dvc  |               |       |
    #                 |               |    unrelated1.dvc
    #    dir/subdir/dir_file_copy.dvc |
    #                  |              |
    #                  |        dir/origin_copy_2.dvc
    #                  |            |
    #                   \          /
    #                    \        /
    #                   dir/Dvcfile
    tmp_dir.gen(
        {
            "origin_data": "origin data content",
            "data_dir": {"dir_file": "dir file content"},
            "dir": {"subdir": {}},
        }
    )

    stages = {}

    origin_copy = tmp_dir / "origin_copy"
    stage = run_copy("origin_data", os.fspath(origin_copy), single_stage=True)
    assert stage is not None
    assert origin_copy.read_text() == "origin data content"
    stages["origin_copy"] = stage

    origin_copy_2 = tmp_dir / "dir" / "origin_copy_2"
    stage = run_copy(
        os.fspath(origin_copy),
        os.fspath(origin_copy_2),
        fname=os.fspath(origin_copy_2) + ".dvc",
        single_stage=True,
    )
    assert stage is not None
    assert origin_copy_2.read_text() == "origin data content"
    stages["origin_copy_2"] = stage

    dir_file_path = tmp_dir / "data_dir" / "dir_file"
    dir_file_copy = tmp_dir / "dir" / "subdir" / "dir_file_copy"
    stage = run_copy(
        os.fspath(dir_file_path),
        os.fspath(dir_file_copy),
        fname=os.fspath(dir_file_copy) + ".dvc",
        single_stage=True,
    )
    assert stage is not None
    assert dir_file_copy.read_text() == "dir file content"
    stages["dir_file_copy"] = stage

    last_stage = tmp_dir / "dir" / DVC_FILE
    deps = [os.fspath(origin_copy_2), os.fspath(dir_file_copy)]
    stage = dvc.run(
        cmd="echo {}".format(" ".join(deps)),
        fname=os.fspath(last_stage),
        deps=deps,
        single_stage=True,
    )
    assert stage is not None
    stages["last_stage"] = stage

    # Unrelated are to verify that reproducing `dir` will not trigger them too
    assert run_copy(os.fspath(origin_copy), "unrelated1", single_stage=True) is not None
    assert (
        run_copy(os.fspath(dir_file_path), "unrelated2", single_stage=True) is not None
    )

    return stages


def _rewrite_file(path_elements, new_content):
    if isinstance(path_elements, str):
        path_elements = [path_elements]
    file = Path(os.sep.join(path_elements))
    file.unlink()
    file.write_text(new_content, encoding="utf-8")


def _read_out(stage):
    return Path(stage.outs[0].fspath).read_text(encoding="utf-8")


def test_recursive_repro_default(dvc, repro_dir):
    """
    Test recursive repro on dir after a dep outside this dir has changed.
    """
    _rewrite_file("origin_data", "new origin data content")

    stages = dvc.reproduce("dir", recursive=True)

    # Check that the dependency ("origin_copy") and the dependent stages
    # inside the folder have been reproduced ("origin_copy_2", "last_stage")
    assert stages == [
        repro_dir["origin_copy"],
        repro_dir["origin_copy_2"],
        repro_dir["last_stage"],
    ]
    assert _read_out(repro_dir["origin_copy"]) == "new origin data content"
    assert _read_out(repro_dir["origin_copy_2"]) == "new origin data content"


def test_recursive_repro_single(dvc, repro_dir):
    """
    Test recursive single-item repro on dir
    after a dep outside this dir has changed.
    """
    _rewrite_file("origin_data", "new origin content")
    _rewrite_file(["data_dir", "dir_file"], "new dir file content")

    stages = dvc.reproduce("dir", recursive=True, single_item=True)
    # Check that just stages inside given dir
    # with changed direct deps have been reproduced.
    # This means that "origin_copy_2" stage should not be reproduced
    # since it depends on "origin_copy".
    # Also check that "dir_file_copy" stage was reproduced before "last_stage"
    assert stages == [repro_dir["dir_file_copy"], repro_dir["last_stage"]]
    assert _read_out(repro_dir["dir_file_copy"]) == "new dir file content"


def test_recursive_repro_single_force(dvc, repro_dir):
    """
    Test recursive single-item force repro on dir
    without any dependencies changing.
    """
    stages = dvc.reproduce("dir", recursive=True, single_item=True, force=True)
    # Check that all stages inside given dir have been reproduced
    # Also check that "dir_file_copy" stage was reproduced before "last_stage"
    # and that "origin_copy" stage was reproduced before "last_stage" stage
    assert len(stages) == 3
    assert set(stages) == {
        repro_dir["origin_copy_2"],
        repro_dir["dir_file_copy"],
        repro_dir["last_stage"],
    }
    assert stages.index(repro_dir["origin_copy_2"]) < stages.index(
        repro_dir["last_stage"]
    )
    assert stages.index(repro_dir["dir_file_copy"]) < stages.index(
        repro_dir["last_stage"]
    )


def test_recursive_repro_empty_dir(tmp_dir, dvc):
    """
    Test recursive repro on an empty directory
    """
    (tmp_dir / "emptydir").mkdir()

    stages = dvc.reproduce("emptydir", recursive=True, force=True)
    assert stages == []


def test_recursive_repro_recursive_missing_file(dvc):
    """
    Test recursive repro on a missing file
    """
    with pytest.raises(StageFileDoesNotExistError):
        dvc.reproduce("notExistingStage.dvc", recursive=True)
    with pytest.raises(StageFileDoesNotExistError):
        dvc.reproduce("notExistingDir/", recursive=True)


def test_recursive_repro_on_stage_file(dvc, repro_dir):
    """
    Test recursive repro on a stage file instead of directory
    """
    stages = dvc.reproduce(
        repro_dir["origin_copy_2"].relpath, recursive=True, force=True
    )
    assert stages == [repro_dir["origin_copy"], repro_dir["origin_copy_2"]]


def test_dvc_formatting_retained(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo content")
    stage = run_copy("foo", "foo_copy", fname="foo_copy.dvc", single_stage=True)
    stage_path = tmp_dir / stage.relpath

    # Add comments and custom formatting to DVC-file
    lines = list(map(_format_dvc_line, stage_path.read_text().splitlines()))
    lines.insert(0, "# Starting comment")
    stage_text = "".join(line + "\n" for line in lines)
    stage_path.write_text(stage_text)

    # Rewrite data source and repro
    (tmp_dir / "foo").write_text("new foo")
    dvc.reproduce("foo_copy.dvc", force=True)

    def _hide_md5(text):
        return re.sub(r"\b[a-f0-9]{32}\b", "<md5>", text)

    def _hide_size(text):
        return re.sub(r"size: [0-9]*\b", "size: <size>", text)

    def _mask(text):
        return _hide_size(_hide_md5(text))

    assert _mask(stage_text) == _mask(stage_path.read_text())


def _format_dvc_line(line):
    # Add line comment for all cache and md5 keys
    if "cache:" in line or "md5:" in line:
        return line + " # line comment"
    # Format command as one word per line
    if line.startswith("cmd: "):
        pre, command = line.split(None, 1)
        return pre + " >\n" + "\n".join("  " + s for s in command.split())
    return line


def test_downstream(dvc):
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
    assert main(["run", "--single-stage", "-o", "A", "echo A>A"]) == 0
    assert main(["run", "--single-stage", "-d", "A", "-o", "B", "echo B>B"]) == 0
    assert main(["run", "--single-stage", "-d", "A", "-o", "C", "echo C>C"]) == 0
    assert (
        main(
            [
                "run",
                "--single-stage",
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
    assert main(["run", "--single-stage", "-o", "G", "echo G>G"]) == 0
    assert main(["run", "--single-stage", "-d", "G", "-o", "F", "echo F>F"]) == 0
    assert (
        main(
            [
                "run",
                "--single-stage",
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
    evaluation = dvc.reproduce("B.dvc", downstream=True, force=True)

    assert len(evaluation) == 3
    assert evaluation[0].relpath == "B.dvc"
    assert evaluation[1].relpath == "D.dvc"
    assert evaluation[2].relpath == "E.dvc"

    # B, C should be run (in any order) before D
    # See https://github.com/iterative/dvc/issues/3602
    evaluation = dvc.reproduce("A.dvc", downstream=True, force=True)

    assert len(evaluation) == 5
    assert evaluation[0].relpath == "A.dvc"
    assert {evaluation[1].relpath, evaluation[2].relpath} == {"B.dvc", "C.dvc"}
    assert evaluation[3].relpath == "D.dvc"
    assert evaluation[4].relpath == "E.dvc"


def test_repro_when_cmd_changes(tmp_dir, dvc, run_copy, mocker):
    from dvc.dvcfile import SingleStageFile

    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", single_stage=True)
    assert not dvc.reproduce(stage.addressing)

    from dvc.stage.run import cmd_run

    m = mocker.patch("dvc.stage.run.cmd_run", wraps=cmd_run)

    data = SingleStageFile(dvc, stage.path)._load()[0]
    data["cmd"] = "  ".join(stage.cmd.split())  # change cmd spacing by two
    (tmp_dir / stage.path).dump(data)

    assert dvc.status([stage.addressing]) == {stage.addressing: ["changed checksum"]}
    assert dvc.reproduce(stage.addressing)[0] == stage
    m.assert_called_once_with(stage, checkpoint_func=None, dry=False, run_env=None)
