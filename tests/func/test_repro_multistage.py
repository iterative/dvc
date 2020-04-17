import os
from textwrap import dedent

import pytest

from dvc.stage import PipelineStage
from tests.func import test_repro

from dvc.main import main


COPY_SCRIPT_FORMAT = dedent(
    """\
    import sys
    import shutil
    shutil.copyfile({}, {})
"""
)
COPY_SCRIPT = COPY_SCRIPT_FORMAT.format("sys.argv[1]", "sys.argv[2]")


class MultiStageRun:
    def _run(self, **kwargs):
        assert kwargs.get("name")
        kwargs.pop("fname", None)
        # ignore fname for now
        return self.dvc.run(**kwargs)

    @staticmethod
    def _get_stage_target(stage):
        return stage.path + ":" + stage.name


class TestReproFailMultiStage(MultiStageRun, test_repro.TestReproFail):
    pass


class TestReproCyclicGraphMultiStage(
    MultiStageRun, test_repro.TestReproCyclicGraph
):
    # TODO: Also test with new-style forced dump
    pass


class TestReproUnderDirMultiStage(
    MultiStageRun, test_repro.TestReproDepUnderDir
):
    pass


class TestReproDepDirWithOutputsUnderItMultiStage(
    MultiStageRun, test_repro.TestReproDepDirWithOutputsUnderIt
):
    pass


class TestReproNoDepsMultiStage(MultiStageRun, test_repro.TestReproNoDeps):
    pass


class TestReproForceMultiStage(MultiStageRun, test_repro.TestReproForce):
    pass


class TestReproChangedCodeMultiStage(
    MultiStageRun, test_repro.TestReproChangedCode
):
    pass


class TestReproChangedDataMultiStage(
    MultiStageRun, test_repro.TestReproChangedData
):
    pass


class TestReproDry(MultiStageRun, test_repro.TestReproDry):
    pass


class TestReproUpToDateMultiStage(MultiStageRun, test_repro.TestReproUpToDate):
    pass


class TestReproChangedDeepDataMultiStage(
    MultiStageRun, test_repro.TestReproChangedDeepData
):
    pass


class TestReproPipelineMultiStage(MultiStageRun, test_repro.TestReproPipeline):
    pass


class TestReproPipelinesMultiStage(
    MultiStageRun, test_repro.TestReproPipelines
):
    pass


class TestReproLockedMultiStage(MultiStageRun, test_repro.TestReproLocked):
    pass


class TestReproLockedCallbackMultiStage(
    MultiStageRun, test_repro.TestReproLockedCallback
):
    pass


class TestReproLockedUnchangedMultiStage(
    MultiStageRun, test_repro.TestReproLockedUnchanged
):
    pass


class TestReproPhonyMultiStage(MultiStageRun, test_repro.TestReproPhony):
    pass


class TestCmdReproMultiStage(MultiStageRun, test_repro.TestCmdRepro):
    pass


class TestReproAllPipelinesMultiStage(
    MultiStageRun, test_repro.TestReproAllPipelines
):
    pass


class TestReproNoCommit(MultiStageRun, test_repro.TestReproNoCommit):
    pass


class TestNonExistingOutputMultiStage(
    MultiStageRun, test_repro.TestNonExistingOutput
):
    pass


class TestReproAlreadyCachedMultiStage(
    MultiStageRun, test_repro.TestReproAlreadyCached
):
    pass


class TestReproChangedDirMultiStage(
    MultiStageRun, test_repro.TestReproChangedDir
):
    pass


class TestReproChangedDirDataMultiStage(
    MultiStageRun, test_repro.TestReproChangedDirData
):
    pass


def test_non_existing_stage_name(tmp_dir, dvc, run_copy):
    from dvc.exceptions import DvcException

    tmp_dir.gen("file1", "file1")
    run_copy("file1", "file2", name="copy-file1-file2")

    with pytest.raises(DvcException):
        dvc.lock_stage(":copy-file1-file3")

    assert main(["lock", ":copy-file1-file3"]) != 0


# TODO: TestReproWorkingDirectoryAsOutput


def test_downstream(tmp_dir, dvc):
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
    assert main(["run", "-n", "A-gen", "-o", "A", "echo A>A"]) == 0
    assert main(["run", "-n", "B-gen", "-d", "A", "-o", "B", "echo B>B"]) == 0
    assert main(["run", "-d", "A", "-o", "C", "echo C>C"]) == 0
    assert (
        main(
            ["run", "-n", "D-gen", "-d", "B", "-d", "C", "-o", "D", "echo D>D"]
        )
        == 0
    )
    assert main(["run", "-o", "G", "echo G>G"]) == 0
    assert main(["run", "-n", "F-gen", "-d", "G", "-o", "F", "echo F>F"]) == 0
    assert main(["run", "-d", "D", "-d", "F", "-o", "E", "echo E>E"]) == 0

    # We want the evaluation to move from B to E
    #
    #       E
    #      /
    #     D
    #    /
    #   B
    #
    evaluation = dvc.reproduce("Dvcfile:B-gen", downstream=True, force=True)

    assert len(evaluation) == 3
    assert (
        isinstance(evaluation[0], PipelineStage)
        and evaluation[0].relpath == "Dvcfile"
        and evaluation[0].name == "B-gen"
    )
    assert (
        isinstance(evaluation[1], PipelineStage)
        and evaluation[1].relpath == "Dvcfile"
        and evaluation[1].name == "D-gen"
    )
    assert (
        not isinstance(evaluation[2], PipelineStage)
        and evaluation[2].relpath == "E.dvc"
    )


def test_repro_when_cmd_changes(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import Dvcfile

    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", fname="copy-process.dvc", name="copy-file")
    target = "copy-process.dvc:copy-file"
    assert not dvc.reproduce(target)

    stage.cmd = "  ".join(stage.cmd.split())  # change cmd spacing by two
    Dvcfile(dvc, "copy-process.dvc").dump_multistage_dvcfile(stage)

    assert dvc.reproduce(target)[0] == stage


def test_repro_when_new_deps_is_added_in_dvcfile(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import Dvcfile
    from dvc.utils.stage import dump_stage_file

    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    stage = dvc.run(
        cmd="python copy.py {} {}".format("foo", "foobar"),
        outs=["foobar"],
        deps=["foo"],
        fname="copy-process.dvc",
        name="copy-file",
    )
    target = "copy-process.dvc:copy-file"
    assert not dvc.reproduce(target)

    dvcfile = Dvcfile(dvc, stage.path)
    data, _ = dvcfile._load()
    data["stages"]["copy-file"]["deps"] += ["copy.py"]
    dump_stage_file(stage.path, data)

    assert dvc.reproduce(target)[0] == stage


def test_repro_when_new_outs_is_added_in_dvcfile(tmp_dir, dvc):
    from dvc.dvcfile import Dvcfile
    from dvc.utils.stage import dump_stage_file

    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    stage = dvc.run(
        cmd="python copy.py {} {}".format("foo", "foobar"),
        outs=[],  # scenario where user forgot to add
        deps=["foo"],
        fname="copy-process.dvc",
        name="copy-file",
    )
    target = "copy-process.dvc:copy-file"
    assert not dvc.reproduce(target)

    dvcfile = Dvcfile(dvc, stage.path)
    data, _ = dvcfile._load()
    data["stages"]["copy-file"]["outs"] = ["foobar"]
    dump_stage_file(stage.path, data)

    assert dvc.reproduce(target)[0] == stage


def test_repro_when_new_deps_is_moved(tmp_dir, dvc):
    from dvc.dvcfile import Dvcfile
    from dvc.utils.stage import dump_stage_file

    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen({"foo": "foo", "bar": "foo"})
    stage = dvc.run(
        cmd="python copy.py {} {}".format("foo", "foobar"),
        outs=["foobar"],
        deps=["foo"],
        fname="copy-process.dvc",
        name="copy-file",
    )
    target = "copy-process.dvc:copy-file"
    assert not dvc.reproduce(target)

    tmp_dir.gen("copy.py", COPY_SCRIPT_FORMAT.format("'bar'", "'foobar'"))
    from shutil import move

    move("foo", "bar")

    dvcfile = Dvcfile(dvc, stage.path)
    data, _ = dvcfile._load()
    data["stages"]["copy-file"]["deps"] = ["bar"]
    dump_stage_file(stage.path, data)

    assert dvc.reproduce(target)[0] == stage


def test_repro_when_new_out_overlaps_others_stage_outs(tmp_dir, dvc):
    from dvc.utils.stage import dump_stage_file
    from dvc.exceptions import OverlappingOutputPathsError

    tmp_dir.gen({"dir": {"file1": "file1"}, "foo": "foo"})
    dvc.add("dir")
    dump_stage_file(
        "Dvcfile",
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


def test_repro_when_new_deps_added_does_not_exist(tmp_dir, dvc):
    from dvc.utils.stage import dump_stage_file
    from dvc.exceptions import ReproductionError

    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("foo", "foo")
    dump_stage_file(
        "Dvcfile",
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


def test_repro_when_new_outs_added_does_not_exist(tmp_dir, dvc):
    from dvc.utils.stage import dump_stage_file
    from dvc.exceptions import ReproductionError

    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("foo", "foo")
    dump_stage_file(
        "Dvcfile",
        {
            "stages": {
                "run-copy": {
                    "cmd": "python copy {} {}".format("foo", "foobar"),
                    "deps": ["foo"],
                    "outs": ["foobar", "bar"],
                }
            }
        },
    )
    with pytest.raises(ReproductionError):
        dvc.reproduce(":run-copy")


def test_repro_when_lockfile_gets_deleted(tmp_dir, dvc):
    from dvc.utils.stage import dump_stage_file

    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("foo", "foo")
    dump_stage_file(
        "Dvcfile",
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
    assert os.path.exists("Dvcfile.lock")
    assert os.path.exists("foobar.dvc")

    assert not dvc.reproduce(":run-copy")
    os.unlink("Dvcfile.lock")
    stages = dvc.reproduce(":run-copy")
    assert (
        stages
        and stages[0].relpath == "Dvcfile"
        and stages[0].name == "run-copy"
    )

    assert os.path.exists("foobar.dvc")
