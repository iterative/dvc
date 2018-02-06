import os

from dvc.main import main
from dvc.stage import Stage
from dvc.output import CmdOutputOutsideOfRepoError
from dvc.project import StageNotFoundError
from dvc.command.remove import CmdRemove

from tests.basic_env import TestDvc


class TestRemove(TestDvc):
    def test(self):
        stage = self.dvc.add(self.FOO)
        stages_removed = self.dvc.remove(self.FOO)

        self.assertEqual(len(stages_removed), 1)
        self.assertIsInstance(stages_removed[0], Stage)
        self.assertEqual(stage.path, stages_removed[0].path)


class TestRemoveNonExistentFile(TestDvc):
    def test(self):
        with self.assertRaises(StageNotFoundError) as cx:
            self.dvc.remove('non_existent_file')


class TestRemoveFileOutsideOfRepo(TestDvc):
    def test(self):
        with self.assertRaises(CmdOutputOutsideOfRepoError) as cx:
            self.dvc.remove(os.path.join(os.path.dirname(self.dvc.root_dir), self.FOO))


class TestCmdRemove(TestDvc):
    def test(self):
        self.dvc.add(self.FOO)
        ret = main(['remove',
                    self.FOO])
        self.assertEqual(ret, 0)

        ret = main(['remove',
                    'non-existing-file'])
        self.assertNotEqual(ret, 0)
