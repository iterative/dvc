from __future__ import unicode_literals

import os
from mock import patch

from dvc.main import main


from dvc.scm.base import FileNotInCommitError


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
        out = self.dvc.scm.get_diff_trees(self.a_ref)
        self.assertFalse(out["equal"])
        self.assertEqual(str(self.a_ref), out["b_ref"])
        self.assertEqual(str(self.git.head.commit), out["a_ref"])


class TestDiffRepo(TestDiff):
    def test(self):
        new_ref = self.git.head.commit
        msg = self.dvc.diff(self.new_file, a_ref=self.a_ref)
        test_msg = "dvc diff from {} to {}\n\n".format(new_ref, self.a_ref)
        test_msg += "diff for {}\n".format(self.new_file)
        test_msg += "-new_file with md5 25fa8325e4e0eb8180445e42558e60bd\n"
        test_msg += "deleted file with size -8 Bytes"
        self.assertEqual(test_msg, msg)


class TestDiffCmdLine(TestDiff):
    def test(self):
        with patch("dvc.repo.Repo", config="testing") as MockRepo:
            MockRepo.return_value.diff.return_value = "testing"
            MockRepo.return_value.diff.return_value = "testing"
            ret = main(["diff", "-t", self.new_file, str(self.a_ref)])
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
        out = self.dvc.scm.get_diff_trees(self.a_ref)
        self.assertFalse(out["equal"])
        self.assertEqual(str(self.a_ref), out["b_ref"])
        self.assertEqual(str(self.git.head.commit), out["a_ref"])


class TestDiffDirRepo(TestDiffDir):
    def test(self):
        new_ref = self.git.head.commit
        msg = self.dvc.diff(self.DATA_DIR, a_ref=self.a_ref)
        test_msg = "dvc diff from ".format(self.DATA_DIR)
        test_msg += "{} to {}\n\n".format(new_ref, self.a_ref)
        test_msg += "-data_dir with md5 d5782f3072167ad3a53ee80b92b30718.dir\n"
        test_msg += (
            "+data_dir with md5 bff5a787d16460f32e9f2e62b183b1cc.dir\n\n"
        )
        test_msg += "2 files not changed, 0 files modified, 0 files added, "
        test_msg += "1 file deleted, size was decreased by 30 Bytes"
        self.assertEqual(
            msg, test_msg, msg="msg is %s test_msg is %s" % (msg, test_msg)
        )


class TestDiffFileNotFound(TestDiffDir):
    def setUp(self):
        super(TestDiffFileNotFound, self).setUp()
        self.unknown_file = "unknown_file_" + str(id(self))

    def test(self):
        with self.assertRaises(FileNotInCommitError):
            self.dvc.diff(self.unknown_file, a_ref=self.a_ref)
