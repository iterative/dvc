from __future__ import unicode_literals

import os
from mock import patch, Mock

from dvc.main import main


from dvc.scm.base import FileNotInCommitError
import dvc.repo.diff as diff
from dvc.command.diff import CmdDiff


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
        self.a_ref = self.git.git.rev_parse(self.git.head.commit, short=True)
        self.new_checksum = _get_checksum(self.dvc, self.new_file)
        self.git.index.add([self.new_file + ".dvc"])
        self.git.index.commit("adds new_file")
        self.test_dct = {
            diff.DIFF_A_REF: self.a_ref,
            diff.DIFF_B_REF: self.git.git.rev_parse(
                self.git.head.commit, short=True
            ),
            diff.DIFF_LIST: [
                {
                    diff.DIFF_TARGET: self.new_file,
                    diff.DIFF_NEW_FILE: self.new_file,
                    diff.DIFF_NEW_CHECKSUM: self.new_checksum,
                    diff.DIFF_SIZE: 13,
                }
            ],
        }

    def test(self):
        out = self.dvc.scm.get_diff_trees(self.a_ref)
        self.assertFalse(out[diff.DIFF_EQUAL])
        self.assertEqual(self.a_ref, out[diff.DIFF_A_REF])
        self.assertEqual(
            self.git.git.rev_parse(self.git.head.commit, short=True),
            out[diff.DIFF_B_REF],
        )


class TestDiffRepo(TestDiff):
    def test(self):
        result = self.dvc.diff(self.a_ref, target=self.new_file)
        self.assertEqual(self.test_dct, result)


class TestDiffCmdLine(TestDiff):
    def test(self):
        with patch("dvc.cli.diff.CmdDiff._show", autospec=True):
            with patch("dvc.repo.Repo", config="testing") as MockRepo:
                MockRepo.return_value.diff.return_value = "testing"
                ret = main(["diff", "-t", self.new_file, self.a_ref])
                self.assertEqual(ret, 0)


class TestDiffCmdMessage(TestDiff):
    maxDiff = None

    def test(self):
        with patch("dvc.repo.Repo", config="testing"):
            m = Mock()
            cmd_diff = CmdDiff(m)
            msg = cmd_diff._show(self.test_dct)
            test_msg = (
                "dvc diff from {0} to {1}\n\n"
                "diff for '{2}'\n"
                "+{2} with md5 {3}\n\n"
                "added file with size 13 Bytes"
            )
            test_msg = test_msg.format(
                self.test_dct[diff.DIFF_A_REF],
                self.test_dct[diff.DIFF_B_REF],
                self.test_dct[diff.DIFF_LIST][0][diff.DIFF_TARGET],
                self.test_dct[diff.DIFF_LIST][0][diff.DIFF_NEW_CHECKSUM],
            )
            self.assertEqual(test_msg, msg)


class TestDiffDir(TestDvc):
    def setUp(self):
        super(TestDiffDir, self).setUp()

        self.dvc.add(self.DATA_DIR)
        self.git.index.add([self.DATA_DIR + ".dvc"])
        self.git.index.commit("adds data_dir")
        self.a_ref = self.git.git.rev_parse(
            self.dvc.scm.git.head.commit, short=True
        )
        self.old_checksum = _get_checksum(self.dvc, self.DATA_DIR)
        self.new_file = os.path.join(self.DATA_SUB_DIR, diff.DIFF_NEW_FILE)
        self.create(self.new_file, self.new_file)
        self.dvc.add(self.DATA_DIR)
        self.git.index.add([self.DATA_DIR + ".dvc"])
        self.git.index.commit(message="adds data_dir with new_file")
        self.new_checksum = _get_checksum(self.dvc, self.DATA_DIR)

    def test(self):
        out = self.dvc.scm.get_diff_trees(self.a_ref)
        self.assertFalse(out[diff.DIFF_EQUAL])
        self.assertEqual(self.a_ref, out[diff.DIFF_A_REF])
        self.assertEqual(
            self.git.git.rev_parse(self.git.head.commit, short=True),
            out[diff.DIFF_B_REF],
        )


class TestDiffDirRepo(TestDiffDir):
    maxDiff = None

    def test(self):
        result = self.dvc.diff(self.a_ref, target=self.DATA_DIR)
        test_dct = {
            diff.DIFF_A_REF: self.git.git.rev_parse(self.a_ref, short=True),
            diff.DIFF_B_REF: self.git.git.rev_parse(
                self.git.head.commit, short=True
            ),
            diff.DIFF_LIST: [
                {
                    diff.DIFF_CHANGE: 0,
                    diff.DIFF_DEL: 0,
                    diff.DIFF_IDENT: 2,
                    diff.DIFF_MOVE: 0,
                    diff.DIFF_NEW: 1,
                    diff.DIFF_IS_DIR: True,
                    diff.DIFF_TARGET: self.DATA_DIR,
                    diff.DIFF_NEW_FILE: self.DATA_DIR,
                    diff.DIFF_NEW_CHECKSUM: self.new_checksum,
                    diff.DIFF_OLD_FILE: self.DATA_DIR,
                    diff.DIFF_OLD_CHECKSUM: self.old_checksum,
                    diff.DIFF_SIZE: 30,
                }
            ],
        }
        self.assertEqual(test_dct, result)


class TestDiffDirRepoDeletedFile(TestDiffDir):
    maxDiff = None

    def setUp(self):
        super(TestDiffDirRepoDeletedFile, self).setUp()

        self.b_ref = self.a_ref
        self.new_checksum = self.old_checksum
        self.a_ref = str(self.dvc.scm.git.head.commit)
        self.old_checksum = _get_checksum(self.dvc, self.DATA_DIR)

    def test(self):
        result = self.dvc.diff(
            self.a_ref, b_ref=self.b_ref, target=self.DATA_DIR
        )
        test_dct = {
            diff.DIFF_A_REF: self.git.git.rev_parse(self.a_ref, short=True),
            diff.DIFF_B_REF: self.git.git.rev_parse(self.b_ref, short=True),
            diff.DIFF_LIST: [
                {
                    diff.DIFF_CHANGE: 0,
                    diff.DIFF_DEL: 1,
                    diff.DIFF_IDENT: 2,
                    diff.DIFF_MOVE: 0,
                    diff.DIFF_NEW: 0,
                    diff.DIFF_IS_DIR: True,
                    diff.DIFF_TARGET: self.DATA_DIR,
                    diff.DIFF_NEW_FILE: self.DATA_DIR,
                    diff.DIFF_NEW_CHECKSUM: self.new_checksum,
                    diff.DIFF_OLD_FILE: self.DATA_DIR,
                    diff.DIFF_OLD_CHECKSUM: self.old_checksum,
                    diff.DIFF_SIZE: -30,
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
            self.dvc.diff(self.a_ref, target=self.unknown_file)
