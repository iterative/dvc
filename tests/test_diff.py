from __future__ import unicode_literals
import os

from dvc.main import main


from dvc.scm.base import FileNotInRepoError


from tests.basic_env import TestDvc


class TestDiff(TestDvc):
    def setUp(self):
        super(TestDiff, self).setUp()

        self.new_file = "new_file"
        self.create(self.new_file, self.new_file)
        self.dvc.add(self.new_file)
        self.a_ref = self.git.head.commit
        self.git.index.add([self.new_file + ".dvc"])
        self.git.index.commit("adds new_file")

    def test(self):
        out = self.dvc.scm.diff(self.new_file, self.a_ref)
        self.assertTrue(out["deleted_file"])
        self.assertEqual("new_file.dvc", out["file_path"])


class TestDiffRepo(TestDiff):
    def test(self):
        msg = self.dvc.diff(self.new_file, a_ref=self.a_ref)
        self.assertTrue("deleted file" in msg)
        self.assertTrue(self.new_file in msg)


class TestDiffCmdLine(TestDiff):
    def test(self):
        ret = main(["diff", self.new_file, str(self.a_ref)])
        self.assertEqual(ret, 0)


class TestDiffDir(TestDvc):
    def setUp(self):
        super(TestDiffDir, self).setUp()

        self.dvc.add(self.DATA_DIR)
        self.git.index.add([self.DATA_DIR + ".dvc"])
        self.git.index.commit("adds data_dir")
        self.a_ref = str(self.dvc.scm.git.head.commit)
        self.new_file = os.path.join(self.DATA_SUB_DIR, "new_file")
        self.create(self.new_file, self.new_file)
        self.dvc.add(self.DATA_DIR)
        self.git.index.add([self.DATA_DIR + ".dvc"])
        self.git.index.commit(message="adds data_dir with new_file")

    def test(self):
        out = self.dvc.scm.diff(self.DATA_DIR, self.a_ref)
        self.assertFalse(out["deleted_file"], "new file added checker failed")
        self.assertEqual(
            self.DATA_DIR + ".dvc",
            out["file_path"],
            "path to file/folder is incorrect",
        )


class TestDiffDirRepo(TestDiffDir):
    def test(self):
        msg = self.dvc.diff(self.DATA_DIR, a_ref=self.a_ref)
        self.assertTrue("1 file deleted" in msg, msg)
        self.assertTrue("size was decreased by " in msg, msg)


class TestDiffFileNotFound(TestDiffDir):
    def setUp(self):
        super(TestDiffFileNotFound, self).setUp()
        self.unknown_file = "unknown_file_" + str(id(self))

    def test(self):
        with self.assertRaises(
            FileNotInRepoError, msg="diff with incorrect arg"
        ) as context:
            self.dvc.scm.diff(self.unknown_file, self.a_ref)
        self.assertTrue(
            "Have not found file/folder" in str(context.exception),
            "scm raises with incorrect error message",
        )


class TestDiffFileNotFoundRepo(TestDiffFileNotFound):
    def test(self):
        with self.assertRaises(
            FileNotInRepoError, msg="diff with incorrect arg"
        ) as context:
            self.dvc.diff(self.unknown_file, a_ref=self.a_ref)
        self.assertTrue("Have not found file/folder" in str(context.exception))
