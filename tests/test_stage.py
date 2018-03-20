from dvc.output import Output
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
        l = [{Output.PARAM_PATH: 'foo', Output.PARAM_MD5: '123'},
             {Output.PARAM_PATH: 'bar', Output.PARAM_MD5: None},
             {Output.PARAM_PATH: 'baz'}]
        d = {Stage.PARAM_DEPS: l}
        Stage.validate(d)

        l[0][Output.PARAM_CACHE] = True
        l[1][Output.PARAM_CACHE] = False
        d = {Stage.PARAM_OUTS: l}
        Stage.validate(d)
