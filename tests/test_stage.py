from dvc.output.local import OutputLOCAL
from dvc.remote.local import RemoteLOCAL
from dvc.stage import Stage, StageFileFormatError

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
        d = {Stage.PARAM_CMD: 'cmd'}
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
        l = [{OutputLOCAL.PARAM_PATH: 'foo', RemoteLOCAL.PARAM_MD5: '123'},
             {OutputLOCAL.PARAM_PATH: 'bar', RemoteLOCAL.PARAM_MD5: None},
             {OutputLOCAL.PARAM_PATH: 'baz'}]
        d = {Stage.PARAM_DEPS: l}
        Stage.validate(d)

        l[0][OutputLOCAL.PARAM_CACHE] = True
        l[1][OutputLOCAL.PARAM_CACHE] = False
        d = {Stage.PARAM_OUTS: l}
        Stage.validate(d)


class TestReload(TestDvc):
    def test(self):
        import yaml
        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)

        with open(stage.relpath, 'r') as fobj:
            d = yaml.safe_load(fobj)

        # NOTE: checking that reloaded stage didn't change its checksum
        md5 = '11111111111111111111111111111111'
        d[stage.PARAM_MD5] = md5

        with open(stage.relpath, 'w') as fobj:
            yaml.safe_dump(d, fobj, default_flow_style=False)

        stage = Stage.load(self.dvc, stage.relpath)
        self.assertTrue(stage is not None)
        stage.dump()

        with open(stage.relpath, 'r') as fobj:
            d = yaml.safe_load(fobj)

        self.assertEqual(d[stage.PARAM_MD5], md5)
