from dvc.main import main

from tests.basic_env import TestDvc
from tests.test_repro import TestRepro, TestReproChangedDeepData


class TestPipelineShowSingle(TestDvc):
    def setUp(self):
        super(TestPipelineShowSingle, self).setUp()
        self.stage = 'foo.dvc'
        ret = main(['add', self.FOO])
        self.assertEqual(ret, 0)

    def test(self):
        ret = main(['pipeline', 'show', self.stage])
        self.assertEqual(ret, 0)

    def test_commands(self):
        ret = main(['pipeline', 'show', self.stage, '--commands'])
        self.assertEqual(ret, 0)        

    def test_outs(self):
        ret = main(['pipeline', 'show', self.stage, '--outs'])
        self.assertEqual(ret, 0)        

    def test_ascii(self):
        ret = main(['pipeline', 'show', '--ascii', self.stage])
        self.assertEqual(ret, 0)

    def test_ascii_commands(self):
        ret = main(['pipeline', 'show', '--ascii', self.stage, '--commands'])
        self.assertEqual(ret, 0)

    def test_ascii_outs(self):
        ret = main(['pipeline', 'show', '--ascii', self.stage, '--outs'])
        self.assertEqual(ret, 0)

    def test_not_dvc_file(self):
        ret = main(['pipeline', 'show', self.FOO])
        self.assertNotEqual(ret, 0)

    def test_non_existing(self):
        ret = main(['pipeline', 'show', 'non-existing'])
        self.assertNotEqual(ret, 0)


class TestPipelineShow(TestRepro):
    def test(self):
        ret = main(['pipeline', 'show', self.file1_stage])
        self.assertEqual(ret, 0)

    def test_commands(self):
        ret = main(['pipeline', 'show', self.file1_stage, '--commands'])
        self.assertEqual(ret, 0)        

    def test_outs(self):
        ret = main(['pipeline', 'show', self.file1_stage, '--outs'])
        self.assertEqual(ret, 0)        

    def test_ascii(self):
        ret = main(['pipeline', 'show', '--ascii', self.file1_stage])
        self.assertEqual(ret, 0)

    def test_ascii_commands(self):
        ret = main(['pipeline', 'show', '--ascii', self.file1_stage, '--commands'])
        self.assertEqual(ret, 0)

    def test_ascii_outs(self):
        ret = main(['pipeline', 'show', '--ascii', self.file1_stage, '--outs'])
        self.assertEqual(ret, 0)

    def test_not_dvc_file(self):
        ret = main(['pipeline', 'show', self.file1])
        self.assertNotEqual(ret, 0)

    def test_non_existing(self):
        ret = main(['pipeline', 'show', 'non-existing'])
        self.assertNotEqual(ret, 0)


class TestPipelineShowDeep(TestReproChangedDeepData):
    def test(self):
        ret = main(['pipeline', 'show', self.file1_stage])
        self.assertEqual(ret, 0)

    def test_commands(self):
        ret = main(['pipeline', 'show', self.file1_stage, '--commands'])
        self.assertEqual(ret, 0)        

    def test_outs(self):
        ret = main(['pipeline', 'show', self.file1_stage, '--outs'])
        self.assertEqual(ret, 0)        

    def test_ascii(self):
        ret = main(['pipeline', 'show', '--ascii', self.file1_stage])
        self.assertEqual(ret, 0)

    def test_ascii_commands(self):
        ret = main(['pipeline', 'show', '--ascii', self.file1_stage, '--commands'])
        self.assertEqual(ret, 0)

    def test_ascii_outs(self):
        ret = main(['pipeline', 'show', '--ascii', self.file1_stage, '--outs'])
        self.assertEqual(ret, 0)

    def test_not_dvc_file(self):
        ret = main(['pipeline', 'show', self.file1])
        self.assertNotEqual(ret, 0)

    def test_non_existing(self):
        ret = main(['pipeline', 'show', 'non-existing'])
        self.assertNotEqual(ret, 0)
