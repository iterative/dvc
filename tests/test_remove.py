import os

from dvc.stage import Stage, OutputOutsideOfRepoError
from dvc.project import StageNotFoundError

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
        with self.assertRaises(OutputOutsideOfRepoError) as cx:
            self.dvc.remove(os.path.join(os.path.dirname(self.dvc.root_dir), self.FOO))
