import os

from mock import patch

from dvc.exceptions import DvcException
from dvc.main import main
from dvc.stage import Stage
from dvc.stage.exceptions import StageFileDoesNotExistError
from dvc.system import System
from dvc.utils.fs import remove
from tests.basic_env import TestDvc


class TestRemove(TestDvc):
    def test(self):
        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)
        (stage_removed,) = self.dvc.remove(stage.path, outs_only=True)

        self.assertIsInstance(stage_removed, Stage)
        self.assertEqual(stage.path, stage_removed.path)
        self.assertFalse(os.path.isfile(self.FOO))

        (stage_removed,) = self.dvc.remove(stage.path)
        self.assertIsInstance(stage_removed, Stage)
        self.assertEqual(stage.path, stage_removed.path)
        self.assertFalse(os.path.isfile(self.FOO))
        self.assertFalse(os.path.exists(stage.path))


class TestRemoveNonExistentFile(TestDvc):
    def test(self):
        with self.assertRaises(StageFileDoesNotExistError):
            self.dvc.remove("non_existent_dvc_file")


class TestRemoveBrokenSymlink(TestDvc):
    def test(self):
        ret = main(["config", "cache.type", "symlink"])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        remove(self.dvc.cache.local.cache_dir)

        self.assertTrue(System.is_symlink(self.FOO))

        ret = main(["remove", self.FOO + ".dvc"])
        self.assertEqual(ret, 0)

        self.assertFalse(os.path.lexists(self.FOO))


class TestRemoveDirectory(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        stage_add = stages[0]
        self.assertTrue(stage_add is not None)
        (stage_removed,) = self.dvc.remove(stage_add.path)
        self.assertEqual(stage_add.path, stage_removed.path)
        self.assertFalse(os.path.exists(self.DATA_DIR))
        self.assertFalse(os.path.exists(stage_removed.path))


class TestCmdRemove(TestDvc):
    def test(self):
        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)
        ret = main(["remove", stage.path])
        self.assertEqual(ret, 0)

        ret = main(["remove", "non-existing-dvc-file"])
        self.assertNotEqual(ret, 0)


class TestRemovePurge(TestDvc):
    def test(self):
        dvcfile = self.dvc.add(self.FOO)[0].path
        ret = main(["remove", "--purge", "--force", dvcfile])

        self.assertEqual(ret, 0)
        self.assertFalse(os.path.exists(self.FOO))
        self.assertFalse(os.path.exists(dvcfile))

    @patch("dvc.prompt.confirm", return_value=False)
    def test_force(self, mock_prompt):
        dvcfile = self.dvc.add(self.FOO)[0].path
        ret = main(["remove", "--purge", dvcfile])

        mock_prompt.assert_called()
        self.assertEqual(ret, 1)
        self.assertRaises(DvcException)


def test_remove_pipeline_outs(tmp_dir, dvc, run_copy):
    from dvc.dvcfile import PIPELINE_FILE

    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")

    assert main(["remove", stage.path]) == 0
    assert not (tmp_dir / "bar").exists()
    assert (tmp_dir / PIPELINE_FILE).exists()

    stage = run_copy("foo", "foobar", name="copy-foo-foobar")

    assert main(["remove", stage.addressing]) == 0
    assert not (tmp_dir / "foobar").exists()
    assert (tmp_dir / "foo").exists()

    stage = run_copy("foo", "baz", name="copy-foo-baz")
    assert main(["remove", "--purge", "-f", stage.addressing]) == 0
    assert not (tmp_dir / "baz").exists()
    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / PIPELINE_FILE).exists()

    dvc.reproduce(PIPELINE_FILE)
    assert main(["remove", PIPELINE_FILE]) == 0
    for file in ["bar", "foobar", "baz"]:
        assert not (tmp_dir / file).exists()

    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / PIPELINE_FILE).exists()
