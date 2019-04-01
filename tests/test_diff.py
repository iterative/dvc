from __future__ import unicode_literals

import os
from mock import patch

from dvc.main import main


from dvc.scm.base import FileNotInCommitError
import dvc.scm.base as git


from tests.basic_env import TestDvc


def _get_checksum(repo, file_name):
    outs = [out for s in repo.stages() for out in s.outs]
    for out in outs:
        if out.url == file_name:
            return out.checksum


class TestDiff(TestDvc):
    def setUp(self):
        super(TestDiff, self).setUp()

        self.new_file = "new_test_file"
        self.create(self.new_file, self.new_file)
        self.dvc.add(self.new_file)
        self.a_ref = self.git.head.commit
        self.new_checksum = _get_checksum(self.dvc, self.new_file)
        self.git.index.add([self.new_file + ".dvc"])
        self.git.index.commit("adds new_file")

    def test(self):
        out = self.dvc.scm.get_diff_trees(self.a_ref)
        self.assertFalse(out[git.DIFF_EQUAL])
        self.assertEqual(str(self.a_ref), out[git.DIFF_A_REF])
        self.assertEqual(str(self.git.head.commit), out[git.DIFF_B_REF])


class TestDiffRepo(TestDiff):
    def test(self):
        result = self.dvc.diff(self.new_file, a_ref=self.a_ref)
        test_dct = {
            git.DIFF_A_REF: str(self.a_ref),
            git.DIFF_B_REF: str(self.git.head.commit),
            git.DIFF_LIST: [
                {
                    git.DIFF_TARGET: self.new_file,
                    git.DIFF_NEW_FILE: self.new_file,
                    git.DIFF_NEW_CHECKSUM: self.new_checksum,
                    git.DIFF_SIZE: 13,
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
        self.old_checksum = _get_checksum(self.dvc, self.DATA_DIR)
        self.new_file = os.path.join(self.DATA_SUB_DIR, git.DIFF_NEW_FILE)
        self.create(self.new_file, self.new_file)
        self.dvc.add(self.DATA_DIR)
        self.git.index.add([self.DATA_DIR + ".dvc"])
        self.git.index.commit(message="adds data_dir with new_file")
        self.new_checksum = _get_checksum(self.dvc, self.DATA_DIR)

    def test(self):
        out = self.dvc.scm.get_diff_trees(self.a_ref)
        self.assertFalse(out[git.DIFF_EQUAL])
        self.assertEqual(str(self.a_ref), out[git.DIFF_A_REF])
        self.assertEqual(str(self.git.head.commit), out[git.DIFF_B_REF])


class TestDiffDirRepo(TestDiffDir):
    maxDiff = None

    def test(self):
        result = self.dvc.diff(self.DATA_DIR, a_ref=self.a_ref)
        test_dct = {
            git.DIFF_A_REF: str(self.a_ref),
            git.DIFF_B_REF: str(self.git.head.commit),
            git.DIFF_LIST: [
                {
                    git.DIFF_CHANGE: 0,
                    git.DIFF_DEL: 0,
                    git.DIFF_IDENT: 2,
                    git.DIFF_MOVE: 0,
                    git.DIFF_NEW: 1,
                    git.DIFF_IS_DIR: True,
                    git.DIFF_TARGET: self.DATA_DIR,
                    git.DIFF_NEW_FILE: self.DATA_DIR,
                    git.DIFF_NEW_CHECKSUM: self.new_checksum,
                    git.DIFF_OLD_FILE: self.DATA_DIR,
                    git.DIFF_OLD_CHECKSUM: self.old_checksum,
                    git.DIFF_SIZE: 30,
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
