import os
import sys

import yaml
import shutil
import filecmp
import collections

from dvc.logger import logger
from dvc.main import main
from dvc.repo import Repo as DvcRepo
from dvc.system import System
from tests.basic_env import TestDvc
from tests.test_repro import TestRepro
from dvc.stage import Stage
from dvc.remote.local import RemoteLOCAL
from dvc.exceptions import DvcException, ConfirmRemoveError

from mock import patch, call
from tests.utils.logger import MockLoggerHandlers, ConsoleFontColorsRemover


class TestCheckout(TestRepro):
    def setUp(self):
        super(TestCheckout, self).setUp()

        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        self.data_dir_stage = stages[0]
        self.assertTrue(self.data_dir_stage is not None)

        self.orig = "orig"
        shutil.copy(self.FOO, self.orig)
        os.unlink(self.FOO)

        self.orig_dir = "orig_dir"
        shutil.copytree(self.DATA_DIR, self.orig_dir)
        shutil.rmtree(self.DATA_DIR)

    def test(self):
        self.dvc.checkout(force=True)
        self._test_checkout()

    def _test_checkout(self):
        self.assertTrue(os.path.isfile(self.FOO))
        self.assertTrue(filecmp.cmp(self.FOO, self.orig, shallow=False))


class TestCheckoutSingleStage(TestCheckout):
    def test(self):
        ret = main(["checkout", "--force", self.foo_stage.path])
        self.assertEqual(ret, 0)

        ret = main(["checkout", "--force", self.data_dir_stage.path])
        self.assertEqual(ret, 0)

        self._test_checkout()


class TestCheckoutCorruptedCacheFile(TestRepro):
    def test(self):
        cache = self.foo_stage.outs[0].cache

        with open(cache, "a") as fd:
            fd.write("1")

        self.dvc.checkout(force=True)

        self.assertFalse(os.path.isfile(self.FOO))
        self.assertFalse(os.path.isfile(cache))


class TestCheckoutCorruptedCacheDir(TestDvc):
    def test(self):
        # NOTE: using 'copy' so that cache and link don't have same inode
        ret = main(["config", "cache.type", "copy"])
        self.assertEqual(ret, 0)

        self.dvc = DvcRepo(".")
        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        self.assertEqual(len(stages[0].outs), 1)
        out = stages[0].outs[0]

        # NOTE: modifying cache file for one of the files inside the directory
        # to check if dvc will detect that the cache is corrupted.
        entry = self.dvc.cache.local.load_dir_cache(out.checksum)[0]
        checksum = entry[self.dvc.cache.local.PARAM_CHECKSUM]
        cache = self.dvc.cache.local.get(checksum)

        with open(cache, "w+") as fobj:
            fobj.write("1")

        self.dvc.checkout(force=True)

        self.assertFalse(os.path.exists(cache))


class TestCmdCheckout(TestCheckout):
    def test(self):
        ret = main(["checkout", "--force"])
        self.assertEqual(ret, 0)
        self._test_checkout()


class CheckoutBase(TestDvc):
    GIT_IGNORE = ".gitignore"

    def commit_data_file(self, fname, content="random text"):
        with open(fname, "w") as fd:
            fd.write(content)
        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)
        self.dvc.scm.add([fname + ".dvc", ".gitignore"])
        self.dvc.scm.commit("adding " + fname)

    def read_ignored(self):
        return list(
            map(lambda s: s.strip("\n"), open(self.GIT_IGNORE).readlines())
        )

    def outs_info(self, stage):
        FileInfo = collections.namedtuple("FileInfo", "path inode")

        paths = [
            os.path.join(root, file)
            for output in stage.outs
            for root, _, files in os.walk(output.path)
            for file in files
        ]

        return [
            FileInfo(path=path, inode=System.inode(path)) for path in paths
        ]


class TestRemoveFilesWhenCheckout(CheckoutBase):
    def test(self):
        fname = "file_in_a_branch"
        branch_master = "master"
        branch_1 = "b1"

        self.dvc.scm.add(self.dvc.scm.untracked_files())
        self.dvc.scm.commit("add all files")

        # add the file into a separate branch
        self.dvc.scm.checkout(branch_1, True)
        ret = main(["checkout", "--force"])
        self.assertEqual(ret, 0)
        self.commit_data_file(fname)

        # Checkout back in master
        self.dvc.scm.checkout(branch_master)
        self.assertTrue(os.path.exists(fname))

        # Make sure `dvc checkout` removes the file
        # self.dvc.checkout()
        ret = main(["checkout", "--force"])
        self.assertEqual(ret, 0)
        self.assertFalse(os.path.exists(fname))


class TestCheckoutCleanWorkingDir(CheckoutBase):
    @patch("dvc.prompt.confirm", return_value=True)
    def test(self, mock_prompt):
        mock_prompt.return_value = True

        stages = self.dvc.add(self.DATA_DIR)
        stage = stages[0]

        working_dir_change = os.path.join(self.DATA_DIR, "not_cached.txt")
        with open(working_dir_change, "w") as f:
            f.write("not_cached")

        ret = main(["checkout", stage.relpath])
        self.assertEqual(ret, 0)
        self.assertFalse(os.path.exists(working_dir_change))

    @patch("dvc.prompt.confirm", return_value=False)
    def test_force(self, mock_prompt):
        mock_prompt.return_value = False

        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        stage = stages[0]

        working_dir_change = os.path.join(self.DATA_DIR, "not_cached.txt")
        with open(working_dir_change, "w") as f:
            f.write("not_cached")

        ret = main(["checkout", stage.relpath])
        self.assertNotEqual(ret, 0)

        mock_prompt.assert_called()
        self.assertNotEqual(ret, 0)
        self.assertRaises(DvcException)


class TestCheckoutSelectiveRemove(CheckoutBase):
    def test(self):
        # Use copy to test for changes in the inodes
        ret = main(["config", "cache.type", "copy"])
        self.assertEqual(ret, 0)

        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        staged_files = self.outs_info(stage)

        os.remove(staged_files[0].path)

        ret = main(["checkout", "--force", stage.relpath])
        self.assertEqual(ret, 0)

        checkedout_files = self.outs_info(stage)

        self.assertEqual(len(staged_files), len(checkedout_files))
        self.assertEqual(staged_files[0].path, checkedout_files[0].path)
        self.assertNotEqual(staged_files[0].inode, checkedout_files[0].inode)
        self.assertEqual(staged_files[1].inode, checkedout_files[1].inode)


class TestGitIgnoreBasic(CheckoutBase):
    def test(self):
        fname1 = "file_1"
        fname2 = "file_2"
        fname3 = "file_3"

        self.dvc.scm.add(self.dvc.scm.untracked_files())
        self.dvc.scm.commit("add all files")

        self.assertFalse(os.path.exists(self.GIT_IGNORE))

        self.commit_data_file(fname1)
        self.commit_data_file(fname2)
        self.dvc.run(
            cmd="python {} {} {}".format(self.CODE, self.FOO, fname3),
            deps=[self.CODE, self.FOO],
            outs_no_cache=[fname3],
        )

        self.assertTrue(os.path.exists(self.GIT_IGNORE))

        ignored = self.read_ignored()

        self.assertEqual(len(ignored), 2)

        self.assertIn("/" + fname1, ignored)
        self.assertIn("/" + fname2, ignored)


class TestGitIgnoreWhenCheckout(CheckoutBase):
    def test(self):
        fname_master = "file_in_a_master"
        branch_master = "master"
        fname_branch = "file_in_a_branch"
        branch_1 = "b1"

        self.dvc.scm.add(self.dvc.scm.untracked_files())
        self.dvc.scm.commit("add all files")
        self.commit_data_file(fname_master)

        self.dvc.scm.checkout(branch_1, True)
        ret = main(["checkout", "--force"])
        self.assertEqual(ret, 0)
        self.commit_data_file(fname_branch)

        self.dvc.scm.checkout(branch_master)
        ret = main(["checkout", "--force"])
        self.assertEqual(ret, 0)

        ignored = self.read_ignored()

        self.assertEqual(len(ignored), 1)
        self.assertIn("/" + fname_master, ignored)

        self.dvc.scm.checkout(branch_1)
        ret = main(["checkout", "--force"])
        self.assertEqual(ret, 0)
        ignored = self.read_ignored()
        self.assertIn("/" + fname_branch, ignored)


class TestCheckoutMissingMd5InStageFile(TestRepro):
    def test(self):
        with open(self.file1_stage, "r") as fd:
            d = yaml.load(fd)

        del d[Stage.PARAM_OUTS][0][RemoteLOCAL.PARAM_CHECKSUM]
        del d[Stage.PARAM_DEPS][0][RemoteLOCAL.PARAM_CHECKSUM]

        with open(self.file1_stage, "w") as fd:
            yaml.dump(d, fd)

        self.dvc.checkout(force=True)


class TestCheckoutEmptyDir(TestDvc):
    def test(self):
        dname = "empty_dir"
        os.mkdir(dname)

        stages = self.dvc.add(dname)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)
        self.assertEqual(len(stage.outs), 1)

        stage.outs[0].remove()
        self.assertFalse(os.path.exists(dname))

        self.dvc.checkout(force=True)

        self.assertTrue(os.path.isdir(dname))
        self.assertEqual(len(os.listdir(dname)), 0)


class TestCheckoutNotCachedFile(TestDvc):
    def test(self):
        cmd = "python {} {} {}".format(self.CODE, self.FOO, "out")

        self.dvc.add(self.FOO)
        stage = self.dvc.run(
            cmd=cmd, deps=[self.FOO, self.CODE], outs_no_cache=["out"]
        )
        self.assertTrue(stage is not None)

        self.dvc.checkout(force=True)


class TestCheckoutWithDeps(TestRepro):
    def test(self):
        os.unlink(self.FOO)
        os.unlink(self.file1)

        self.assertFalse(os.path.exists(self.FOO))
        self.assertFalse(os.path.exists(self.file1))

        ret = main(["checkout", "--force", self.file1_stage, "--with-deps"])
        self.assertEqual(ret, 0)

        self.assertTrue(os.path.exists(self.FOO))
        self.assertTrue(os.path.exists(self.file1))


class TestCheckoutDirectory(TestRepro):
    def test(self):
        stage = self.dvc.add(self.DATA_DIR)[0]

        shutil.rmtree(self.DATA_DIR)
        self.assertFalse(os.path.exists(self.DATA_DIR))

        ret = main(["checkout", stage.path])
        self.assertEqual(ret, 0)

        self.assertTrue(os.path.exists(self.DATA_DIR))


class TestCheckoutHook(TestDvc):
    @patch("sys.stdout.isatty", return_value=True)
    @patch("dvc.prompt.input", side_effect=EOFError)
    def test(self, mock_input, mock_isatty):
        """ Test that dvc checkout handles EOFError gracefully, which is what
        it will experience when running in a git hook.
        """
        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertNotEqual(stage, None)

        self.create(os.path.join(self.DATA_DIR, "test"), "test")

        with self.assertRaises(ConfirmRemoveError):
            self.dvc.checkout()


class TestCheckoutSuggestGit(TestRepro):
    def test(self):

        try:
            self.dvc.checkout(target="gitbranch")
        except DvcException as exc:
            msg = str(exc)
            self.assertEqual(
                msg,
                (
                    "'gitbranch' does not exist. "
                    "Did you mean 'git checkout gitbranch'?"
                ),
            )

        try:
            self.dvc.checkout(target=self.FOO)
        except DvcException as exc:
            msg = str(exc)
            self.assertEqual(
                msg,
                (
                    "bad stage filename '{}'."
                    " Stage files should be named 'Dvcfile'"
                    " or have a '.dvc' suffix (e.g. '{}.dvc')."
                    " Did you mean 'git checkout {}'?".format(
                        self.FOO, self.FOO, self.FOO
                    )
                ),
            )


class TestCheckoutShouldHaveSelfClearingProgressBar(TestDvc):
    def setUp(self):
        super(TestCheckoutShouldHaveSelfClearingProgressBar, self).setUp()
        self._prepare_repo()
        self._checkout()
        self.data_checkout_pattern = "Data .* didn't change."
        self.clearline_pattern = "\r\x1b\\[K"

    def test(self):
        stdout_calls = self.to_generator(self.stdout_mock.method_calls)

        self.assertThenWriteWithArgMatching(
            stdout_calls, self.data_checkout_pattern
        )
        self.assertThenWriteWithArgMatching(
            stdout_calls, "\\[#{15} {15}\\].*% Checkout in progress"
        )
        self.assertThenWriteWithArgMatching(
            stdout_calls, self.clearline_pattern
        )

        self.assertThenWriteWithArgMatching(
            stdout_calls, self.data_checkout_pattern
        )
        self.assertThenWriteWithArgMatching(
            stdout_calls, "\\[#{30}\\] 100% Checkout in progress"
        )
        self.assertThenWriteWithArgMatching(
            stdout_calls, self.clearline_pattern
        )

    def _prepare_repo(self):
        storage = self.mkdtemp()

        ret = main(["remote", "add", "-d", "myremote", storage])
        self.assertEqual(0, ret)

        ret = main(["add", self.FOO])
        self.assertEqual(0, ret)

        ret = main(["add", self.BAR])
        self.assertEqual(0, ret)

    def _checkout(self):
        with MockLoggerHandlers(
            logger
        ), ConsoleFontColorsRemover(), patch.object(
            sys, "stdout"
        ) as stdout_mock:
            self.stdout_mock = logger.handlers[0].stream = stdout_mock

            ret = main(["checkout"])
            self.assertEqual(0, ret)

    def to_generator(self, calls):
        return (c for c in calls)

    def assertThenWriteWithArgMatching(self, calls, arg_regex):
        next_call = next(calls, None)

        if not next_call:
            raise AssertionError(
                "Expected call to write with arg matching: {}"
                ", but there are  no more calls to "
                "check!".format(arg_regex)
            )

        method_called = next_call[0]

        if method_called == call.write()[0]:
            arg_called = next_call[1][0]
            import re

            pattern = re.compile(arg_regex)
            result = pattern.search(arg_called)
            if result:
                return

        self.assertThenWriteWithArgMatching(calls, arg_regex)
