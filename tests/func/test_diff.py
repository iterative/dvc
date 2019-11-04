from __future__ import unicode_literals

import os

import dvc.repo.diff as diff
from dvc.main import main
from dvc.scm.base import FileNotInCommitError
from tests.basic_env import TestDvcGit


class TestDiff(TestDvcGit):
    def setUp(self):
        super(TestDiff, self).setUp()

        self.new_file = "new_test_file"
        self.create(self.new_file, self.new_file)
        stage = self.dvc.add(self.new_file)[0]
        self.a_ref = self.git.git.rev_parse(self.git.head.commit, short=True)
        self.new_checksum = stage.outs[0].checksum
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
        ret = main(["diff", "-t", self.new_file, self.a_ref])
        self.assertEqual(ret, 0)


class TestDiffDir(TestDvcGit):
    def setUp(self):
        super(TestDiffDir, self).setUp()

        stage = self.dvc.add(self.DATA_DIR)[0]
        self.git.index.add([self.DATA_DIR + ".dvc"])
        self.git.index.commit("adds data_dir")
        self.a_ref = self.git.git.rev_parse(
            self.dvc.scm.repo.head.commit, short=True
        )
        self.old_checksum = stage.outs[0].checksum
        self.new_file = os.path.join(self.DATA_SUB_DIR, diff.DIFF_NEW_FILE)
        self.create(self.new_file, self.new_file)
        stage = self.dvc.add(self.DATA_DIR)[0]
        self.git.index.add([self.DATA_DIR + ".dvc"])
        self.git.index.commit(message="adds data_dir with new_file")
        self.new_checksum = stage.outs[0].checksum

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
        tmp = self.new_checksum
        self.new_checksum = self.old_checksum
        self.a_ref = str(self.dvc.scm.repo.head.commit)
        self.old_checksum = tmp

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


class TestDiffModifiedFile(TestDiff):
    maxDiff = None

    def setUp(self):
        super(TestDiffModifiedFile, self).setUp()

        self.old_checksum = self.new_checksum
        self.new_file_content = "new_test_file_bigger_content_123456789"
        self.diff_len = len(self.new_file) + len(self.new_file_content)
        self.create(self.new_file, self.new_file_content)
        stage = self.dvc.add(self.new_file)[0]
        self.git.index.add([self.new_file + ".dvc"])
        self.git.index.commit("change new_file content to be bigger")
        self.new_checksum = stage.outs[0].checksum
        self.b_ref = self.git.git.rev_parse(self.git.head.commit, short=True)

    def test(self):
        result = self.dvc.diff(
            self.a_ref, b_ref=self.b_ref, target=self.new_file
        )
        test_dct = {
            diff.DIFF_A_REF: self.git.git.rev_parse(self.a_ref, short=True),
            diff.DIFF_B_REF: self.git.git.rev_parse(self.b_ref, short=True),
            diff.DIFF_LIST: [
                {
                    diff.DIFF_NEW_CHECKSUM: self.new_checksum,
                    diff.DIFF_NEW_FILE: self.new_file,
                    diff.DIFF_TARGET: self.new_file,
                    diff.DIFF_SIZE: self.diff_len,
                }
            ],
        }
        self.assertEqual(test_dct, result)


class TestDiffDirWithFile(TestDiffDir):
    maxDiff = None

    def setUp(self):
        super(TestDiffDirWithFile, self).setUp()

        self.a_ref = self.git.git.rev_parse(self.git.head.commit, short=True)
        self.old_checksum = self.new_checksum
        self.new_file_content = "new_test_file_bigger_content_123456789"
        self.diff_len = len(self.new_file_content)
        self.create(self.new_file, self.new_file_content)
        stage = self.dvc.add(self.DATA_DIR)[0]
        self.git.index.add([self.DATA_DIR + ".dvc"])
        self.git.index.commit(message="modify file in the data dir")
        self.new_checksum = stage.outs[0].checksum
        self.b_ref = self.git.git.rev_parse(self.git.head.commit, short=True)

    def test(self):
        result = self.dvc.diff(self.a_ref, target=self.DATA_DIR)
        test_dct = {
            diff.DIFF_A_REF: self.git.git.rev_parse(self.a_ref, short=True),
            diff.DIFF_B_REF: self.git.git.rev_parse(self.b_ref, short=True),
            diff.DIFF_LIST: [
                {
                    diff.DIFF_IDENT: 2,
                    diff.DIFF_CHANGE: 1,
                    diff.DIFF_DEL: 0,
                    diff.DIFF_MOVE: 0,
                    diff.DIFF_NEW: 0,
                    diff.DIFF_IS_DIR: True,
                    diff.DIFF_TARGET: self.DATA_DIR,
                    diff.DIFF_NEW_FILE: self.DATA_DIR,
                    diff.DIFF_NEW_CHECKSUM: self.new_checksum,
                    diff.DIFF_OLD_FILE: self.DATA_DIR,
                    diff.DIFF_OLD_CHECKSUM: self.old_checksum,
                    diff.DIFF_SIZE: self.diff_len,
                }
            ],
        }
        self.assertEqual(test_dct, result)


class TestDiffCmdMessage(TestDiff):
    maxDiff = None

    def test(self):
        ret = main(
            [
                "diff",
                self.test_dct[diff.DIFF_A_REF],
                self.test_dct[diff.DIFF_B_REF],
            ]
        )
        self.assertEqual(ret, 0)

        msg1 = "dvc diff from {0} to {1}".format(
            self.git.git.rev_parse(self.test_dct[diff.DIFF_A_REF], short=True),
            self.git.git.rev_parse(self.test_dct[diff.DIFF_B_REF], short=True),
        )
        msg2 = "diff for '{0}'".format(
            self.test_dct[diff.DIFF_LIST][0][diff.DIFF_TARGET]
        )
        msg3 = "+{0} with md5 {1}".format(
            self.test_dct[diff.DIFF_LIST][0][diff.DIFF_TARGET],
            self.test_dct[diff.DIFF_LIST][0][diff.DIFF_NEW_CHECKSUM],
        )
        msg4 = "added file with size 13 Bytes"
        for m in [msg1, msg2, msg3, msg4]:
            assert m in self._caplog.text
