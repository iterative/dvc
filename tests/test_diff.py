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
        result = self.dvc.diff(self.new_file, a_ref=self.a_ref)
        test_dct = {
            "b_ref": str(self.a_ref),
            "a_ref": str(self.git.head.commit),
            "diffs": [
                {
                    "target": self.new_file,
                    "old_file": self.new_file,
                    "old_checksum": "25fa8325e4e0eb8180445e42558e60bd",
                    "size_diff": -8,
                }
            ],
        }
        self.assertEqual(test_dct, result)


class TestDiffCmdLine(TestDiff):
    def test(self):
        with patch("dvc.cli.diff.CmdDiff._show", autospec=True):
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
        result = self.dvc.diff(self.DATA_DIR, a_ref=self.a_ref)
        test_dct = {
            "b_ref": str(self.a_ref),
            "a_ref": str(self.git.head.commit),
            "diffs": [
                {
                    "changes": 0,
                    "del": 1,
                    "ident": 2,
                    "moves": 0,
                    "new": 0,
                    "target": self.DATA_DIR,
                    "old_file": self.DATA_DIR,
                    "old_checksum": "d5782f3072167ad3a53ee80b92b30718.dir",
                    "new_file": self.DATA_DIR,
                    "new_checksum": "bff5a787d16460f32e9f2e62b183b1cc.dir",
                    "size_diff": -30,
                }
            ],
        }
        self.assertEqual(test_dct, result)


class TestDiffFileNotFound(TestDiffDir):
    def setUp(self):
        super(TestDiffFileNotFound, self).setUp()
        self.unknown_file = "unknown_file_" + str(id(self))

    def test(self):
        with self.assertRaises(FileNotInCommitError):
            self.dvc.diff(self.unknown_file, a_ref=self.a_ref)
