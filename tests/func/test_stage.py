import os
import tempfile
import pytest

from dvc.main import main
from dvc.output.local import LocalOutput
from dvc.remote.local import LocalRemote
from dvc.repo import Repo
from dvc.stage import Stage
from dvc.dvcfile import SingleStageFile
from dvc.stage.exceptions import StageFileFormatError
from dvc.utils.stage import dump_stage_file
from dvc.utils.stage import load_stage_file
from tests.basic_env import TestDvc


def test_cmd_obj():
    with pytest.raises(StageFileFormatError):
        SingleStageFile.validate({Stage.PARAM_CMD: {}})


def test_cmd_none():
    SingleStageFile.validate({Stage.PARAM_CMD: None})


def test_no_cmd():
    SingleStageFile.validate({})


def test_cmd_str():
    SingleStageFile.validate({Stage.PARAM_CMD: "cmd"})


def test_object():
    with pytest.raises(StageFileFormatError):
        SingleStageFile.validate({Stage.PARAM_DEPS: {}})

    with pytest.raises(StageFileFormatError):
        SingleStageFile.validate({Stage.PARAM_OUTS: {}})


def test_none():
    SingleStageFile.validate({Stage.PARAM_DEPS: None})
    SingleStageFile.validate({Stage.PARAM_OUTS: None})


def test_empty_list():
    d = {Stage.PARAM_DEPS: []}
    SingleStageFile.validate(d)

    d = {Stage.PARAM_OUTS: []}
    SingleStageFile.validate(d)


def test_list():
    lst = [
        {LocalOutput.PARAM_PATH: "foo", LocalRemote.PARAM_CHECKSUM: "123"},
        {LocalOutput.PARAM_PATH: "bar", LocalRemote.PARAM_CHECKSUM: None},
        {LocalOutput.PARAM_PATH: "baz"},
    ]
    d = {Stage.PARAM_DEPS: lst}
    SingleStageFile.validate(d)

    lst[0][LocalOutput.PARAM_CACHE] = True
    lst[1][LocalOutput.PARAM_CACHE] = False
    d = {Stage.PARAM_OUTS: lst}
    SingleStageFile.validate(d)


class TestReload(TestDvc):
    def test(self):
        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)

        d = load_stage_file(stage.relpath)

        # NOTE: checking that reloaded stage didn't change its checksum
        md5 = "11111111111111111111111111111111"
        d[stage.PARAM_MD5] = md5
        dump_stage_file(stage.relpath, d)

        dvcfile = SingleStageFile(self.dvc, stage.relpath)
        stage = dvcfile.stage

        self.assertTrue(stage is not None)
        dvcfile.dump(stage)

        d = load_stage_file(stage.relpath)
        self.assertEqual(d[stage.PARAM_MD5], md5)


class TestDefaultWorkingDirectory(TestDvc):
    def test_ignored_in_checksum(self):
        stage = self.dvc.run(
            cmd="echo test > {}".format(self.FOO),
            deps=[self.BAR],
            outs=[self.FOO],
            single_stage=True,
        )

        d = stage.dumpd()
        self.assertNotIn(Stage.PARAM_WDIR, d.keys())

        d = load_stage_file(stage.relpath)
        self.assertNotIn(Stage.PARAM_WDIR, d.keys())

        with self.dvc.lock, self.dvc.state:
            stage = SingleStageFile(self.dvc, stage.relpath).stage
            self.assertFalse(stage.changed())


class TestExternalRemoteResolution(TestDvc):
    def test_remote_output(self):
        tmp_path = tempfile.mkdtemp()
        storage = os.path.join(tmp_path, "storage")
        file_path = os.path.join(storage, "file")

        os.makedirs(storage)

        assert main(["remote", "add", "tmp", tmp_path]) == 0
        assert main(["remote", "add", "storage", "remote://tmp/storage"]) == 0
        assert (
            main(
                [
                    "run",
                    "--single-stage",
                    "-O",
                    "remote://storage/file",
                    "echo file > {path}".format(path=file_path),
                ]
            )
            == 0
        )

        assert os.path.exists(file_path)

    def test_remote_dependency(self):
        tmp_path = tempfile.mkdtemp()
        storage = os.path.join(tmp_path, "storage")
        file_path = os.path.join(storage, "file")

        os.makedirs(storage)

        with open(file_path, "w") as fobj:
            fobj.write("Isle of Dogs")

        assert main(["remote", "add", "tmp", tmp_path]) == 0
        assert main(["remote", "add", "storage", "remote://tmp/storage"]) == 0
        assert main(["import-url", "remote://storage/file", "movie.txt"]) == 0

        assert os.path.exists("movie.txt")


def test_md5_ignores_comments(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen("foo", "foo content")

    with open(stage.path, "a") as f:
        f.write("# End comment\n")

    new_stage = SingleStageFile(dvc, stage.path).stage
    assert not new_stage.stage_changed()


def test_meta_is_preserved(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen("foo", "foo content")

    # Add meta to DVC-file
    data = load_stage_file(stage.path)
    data["meta"] = {"custom_key": 42}
    dump_stage_file(stage.path, data)

    # Loading and dumping to test that it works and meta is retained
    dvcfile = SingleStageFile(dvc, stage.path)
    new_stage = dvcfile.stage
    dvcfile.dump(new_stage)

    new_data = load_stage_file(stage.path)
    assert new_data["meta"] == data["meta"]


def test_parent_repo_collect_stages(tmp_dir, scm, dvc):
    tmp_dir.gen({"subdir": {}})
    subrepo_dir = tmp_dir / "subdir"

    with subrepo_dir.chdir():
        subrepo = Repo.init(subdir=True)
        subrepo_dir.gen("subrepo_file", "subrepo file content")
        subrepo.add("subrepo_file")

    stages = dvc.collect(None)
    subrepo_stages = subrepo.collect(None)

    assert stages == []
    assert subrepo_stages != []


def test_stage_addressing(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    stage1 = run_copy("foo", "bar", single_stage=True)
    assert stage1.addressing == "bar.dvc"

    stage2 = run_copy("bar", "baz", name="copy-bar-baz")
    assert stage2.addressing == "dvc.yaml:copy-bar-baz"

    folder = tmp_dir / "dir"
    folder.mkdir()
    with folder.chdir():
        assert stage1.addressing == os.path.relpath(stage1.path)
        assert (
            stage2.addressing
            == os.path.relpath(stage2.path) + ":" + stage2.name
        )
