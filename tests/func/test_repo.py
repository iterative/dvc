from dvc.ignore import DvcIgnore
from dvc.main import main
from dvc.repo import Repo
from dvc.stage import Stage
from tests.basic_env import TestDvcGit


class TestIgnore(TestDvcGit):
    def _stage_name(self, file):
        return file + Stage.STAGE_FILE_SUFFIX

    def test_should_not_gather_stage_files_from_ignored_dir(self):
        ret = main(["add", self.FOO, self.BAR, self.DATA, self.DATA_SUB])
        self.assertEqual(0, ret)

        stages = self.dvc.stages
        self.assertEqual(4, len(stages))

        self.create(DvcIgnore.DVCIGNORE_FILE, self.DATA_DIR)

        self.dvc = Repo(self.dvc.root_dir)
        stages = self.dvc.stages
        self.assertEqual(2, len(stages))

        stagenames = [s.relpath for s in stages]
        self.assertIn(self._stage_name(self.FOO), stagenames)
        self.assertIn(self._stage_name(self.BAR), stagenames)
        self.assertNotIn(self._stage_name(self.DATA), stagenames)
        self.assertNotIn(self._stage_name(self.DATA_SUB), stagenames)
