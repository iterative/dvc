import tempfile
import os

from dvc.main import main
from dvc.output.local import OutputLOCAL
from dvc.remote.local import RemoteLOCAL
from dvc.stage import Stage, StageFileFormatError
from dvc.utils.stage import load_stage_file, dump_stage_file

from tests.basic_env import TestDvc


class TestSchema(TestDvc):
    def _validate_fail(self, d):
        with self.assertRaises(StageFileFormatError):
            Stage.validate(d)


class TestSchemaCmd(TestSchema):
    def test_cmd_object(self):
        d = {Stage.PARAM_CMD: {}}
        self._validate_fail(d)

    def test_cmd_none(self):
        d = {Stage.PARAM_CMD: None}
        Stage.validate(d)

    def test_no_cmd(self):
        d = {}
        Stage.validate(d)

    def test_cmd_str(self):
        d = {Stage.PARAM_CMD: "cmd"}
        Stage.validate(d)


class TestSchemaDepsOuts(TestSchema):
    def test_object(self):
        d = {Stage.PARAM_DEPS: {}}
        self._validate_fail(d)

        d = {Stage.PARAM_OUTS: {}}
        self._validate_fail(d)

    def test_none(self):
        d = {Stage.PARAM_DEPS: None}
        Stage.validate(d)

        d = {Stage.PARAM_OUTS: None}
        Stage.validate(d)

    def test_empty_list(self):
        d = {Stage.PARAM_DEPS: []}
        Stage.validate(d)

        d = {Stage.PARAM_OUTS: []}
        Stage.validate(d)

    def test_list(self):
        lst = [
            {OutputLOCAL.PARAM_PATH: "foo", RemoteLOCAL.PARAM_CHECKSUM: "123"},
            {OutputLOCAL.PARAM_PATH: "bar", RemoteLOCAL.PARAM_CHECKSUM: None},
            {OutputLOCAL.PARAM_PATH: "baz"},
        ]
        d = {Stage.PARAM_DEPS: lst}
        Stage.validate(d)

        lst[0][OutputLOCAL.PARAM_CACHE] = True
        lst[1][OutputLOCAL.PARAM_CACHE] = False
        d = {Stage.PARAM_OUTS: lst}
        Stage.validate(d)


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

        stage = Stage.load(self.dvc, stage.relpath)
        self.assertTrue(stage is not None)
        stage.dump()

        d = load_stage_file(stage.relpath)
        self.assertEqual(d[stage.PARAM_MD5], md5)


class TestDefaultWorkingDirectory(TestDvc):
    def test_ignored_in_checksum(self):
        stage = self.dvc.run(
            cmd="echo test > {}".format(self.FOO),
            deps=[self.BAR],
            outs=[self.FOO],
        )

        d = stage.dumpd()
        self.assertEqual(d[stage.PARAM_WDIR], ".")

        d = load_stage_file(stage.relpath)
        self.assertEqual(d[stage.PARAM_WDIR], ".")

        del d[stage.PARAM_WDIR]
        dump_stage_file(stage.relpath, d)

        d = load_stage_file(stage.relpath)
        self.assertIsNone(d.get(stage.PARAM_WDIR))

        with self.dvc.state:
            stage = Stage.load(self.dvc, stage.relpath)
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
        assert main(["import", "remote://storage/file", "movie.txt"]) == 0

        assert os.path.exists("movie.txt")


def test_md5_ignores_comments(repo_dir, dvc):
    stage, = dvc.add("foo")

    with open(stage.path, "a") as f:
        f.write("# End comment\n")

    new_stage = Stage.load(dvc, stage.path)
    assert not new_stage.changed_md5()


def test_meta_is_preserved(dvc):
    stage, = dvc.add("foo")

    # Add meta to stage file
    data = load_stage_file(stage.path)
    data["meta"] = {"custom_key": 42}
    dump_stage_file(stage.path, data)

    # Loading and dumping to test that it works and meta is retained
    new_stage = Stage.load(dvc, stage.path)
    new_stage.dump()

    new_data = load_stage_file(stage.path)
    assert new_data["meta"] == data["meta"]
