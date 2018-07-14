from dvc.main import main

from tests.test_repro import TestRepro


class TestPipelineShow(TestRepro):
    def test(self):
        ret = main(['pipeline', 'show', self.file1_stage])
        self.assertEqual(ret, 0)

    def test_commands(self):
        ret = main(['pipeline', 'show', self.file1_stage, '--commands'])
        self.assertEqual(ret, 0)        

    def test_not_dvc_file(self):
        ret = main(['pipeline', 'show', self.file1])
        self.assertNotEqual(ret, 0)

    def test_non_existing(self):
        ret = main(['pipeline', 'show', 'non-existing'])
        self.assertNotEqual(ret, 0)
