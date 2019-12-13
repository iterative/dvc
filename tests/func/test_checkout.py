import collections
import filecmp
import logging
import os
import shutil

import pytest
from mock import patch

from dvc.exceptions import CheckoutError
from dvc.exceptions import CheckoutErrorSuggestGit
from dvc.exceptions import ConfirmRemoveError
from dvc.exceptions import DvcException
from dvc.main import main
from dvc.remote.local import RemoteLOCAL
from dvc.repo import Repo as DvcRepo
from dvc.stage import Stage
from dvc.stage import StageFileBadNameError
from dvc.stage import StageFileDoesNotExistError
from dvc.system import System
from dvc.utils import relpath
from dvc.utils import walk_files
from dvc.utils.compat import is_py2
from dvc.utils.stage import dump_stage_file
from dvc.utils.stage import load_stage_file
from tests.basic_env import TestDvc
from tests.basic_env import TestDvcGit
from tests.func.test_repro import TestRepro

logger = logging.getLogger("dvc")


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
        cache = self.foo_stage.outs[0].cache_path

        with open(cache, "a") as fd:
            fd.write("1")

        with pytest.raises(CheckoutError):
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

        with pytest.raises(CheckoutError):
            self.dvc.checkout(force=True)

        self.assertFalse(os.path.exists(cache))


class TestCmdCheckout(TestCheckout):
    def test(self):
        ret = main(["checkout", "--force"])
        self.assertEqual(ret, 0)
        self._test_checkout()


class CheckoutBase(TestDvcGit):
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
        with open(self.GIT_IGNORE) as f:
            return [s.strip("\n") for s in f.readlines()]

    def outs_info(self, stage):
        FileInfo = collections.namedtuple("FileInfo", "path inode")

        paths = [
            path
            for output in stage["outs"]
            for path in walk_files(output["path"], self.dvc.dvcignore)
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

        ret = main(["add", self.DATA_DIR])
        self.assertEqual(0, ret)

        stage_path = self.DATA_DIR + Stage.STAGE_FILE_SUFFIX
        stage = load_stage_file(stage_path)
        staged_files = self.outs_info(stage)

        # move instead of remove, to lock inode assigned to stage_files[0].path
        # if we were to use remove, we might end up with same inode assigned to
        # newly checked out file
        shutil.move(staged_files[0].path, "random_name")

        ret = main(["checkout", "--force", stage_path])
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
        d = load_stage_file(self.file1_stage)
        del d[Stage.PARAM_OUTS][0][RemoteLOCAL.PARAM_CHECKSUM]
        del d[Stage.PARAM_DEPS][0][RemoteLOCAL.PARAM_CHECKSUM]
        dump_stage_file(self.file1_stage, d)

        with pytest.raises(CheckoutError):
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
            self.dvc.checkout(targets=["gitbranch"])
        except DvcException as exc:
            self.assertIsInstance(exc, CheckoutErrorSuggestGit)
            self.assertIsInstance(exc.cause, StageFileDoesNotExistError)
            self.assertIsNone(exc.cause.cause)

        try:
            self.dvc.checkout(targets=[self.FOO])
        except DvcException as exc:
            self.assertIsInstance(exc, CheckoutErrorSuggestGit)
            self.assertIsInstance(exc.cause, StageFileBadNameError)
            self.assertIsNone(exc.cause.cause)


class TestCheckoutTargetRecursiveShouldNotRemoveOtherUsedFiles(TestDvc):
    def test(self):
        ret = main(["add", self.DATA_DIR, self.FOO, self.BAR])
        self.assertEqual(0, ret)

        ret = main(["checkout", "-R", self.DATA_DIR])
        self.assertEqual(0, ret)

        self.assertTrue(os.path.exists(self.FOO))
        self.assertTrue(os.path.exists(self.BAR))


class TestCheckoutRecursiveNotDirectory(TestDvc):
    def test(self):
        ret = main(["add", self.FOO])
        self.assertEqual(0, ret)

        self.dvc.checkout(targets=[self.FOO + ".dvc"], recursive=True)


class TestCheckoutMovedCacheDirWithSymlinks(TestDvc):
    def test(self):
        ret = main(["config", "cache.type", "symlink"])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["add", self.DATA_DIR])
        self.assertEqual(ret, 0)

        if os.name == "nt" and is_py2:
            from jaraco.windows.filesystem import readlink
        else:
            readlink = os.readlink

        self.assertTrue(System.is_symlink(self.FOO))
        old_foo_link = readlink(self.FOO)

        self.assertTrue(System.is_symlink(self.DATA))
        old_data_link = readlink(self.DATA)

        old_cache_dir = self.dvc.cache.local.cache_dir
        new_cache_dir = old_cache_dir + "_new"
        os.rename(old_cache_dir, new_cache_dir)

        ret = main(["cache", "dir", new_cache_dir])
        self.assertEqual(ret, 0)

        ret = main(["checkout", "-f"])
        self.assertEqual(ret, 0)

        self.assertTrue(System.is_symlink(self.FOO))
        new_foo_link = readlink(self.FOO)

        self.assertTrue(System.is_symlink(self.DATA))
        new_data_link = readlink(self.DATA)

        self.assertEqual(
            relpath(old_foo_link, old_cache_dir),
            relpath(new_foo_link, new_cache_dir),
        )

        self.assertEqual(
            relpath(old_data_link, old_cache_dir),
            relpath(new_data_link, new_cache_dir),
        )


def test_checkout_no_checksum(tmp_dir, dvc):
    tmp_dir.gen("file", "file content")
    stage = dvc.run(outs=["file"], no_exec=True, cmd="somecmd")

    with pytest.raises(CheckoutError):
        dvc.checkout([stage.path], force=True)

    assert not os.path.exists("file")


@pytest.mark.parametrize(
    "link, link_test_func",
    [("hardlink", System.is_hardlink), ("symlink", System.is_symlink)],
)
def test_checkout_relink(tmp_dir, dvc, link, link_test_func):
    dvc.cache.local.cache_types = [link]

    tmp_dir.dvc_gen({"dir": {"data": "text"}})
    dvc.unprotect("dir/data")
    assert not link_test_func("dir/data")

    dvc.checkout(["dir.dvc"], relink=True)
    assert link_test_func("dir/data")


@pytest.mark.parametrize("link", ["hardlink", "symlink", "copy"])
def test_checkout_relink_protected(tmp_dir, dvc, link):
    dvc.cache.local.cache_types = [link]
    dvc.cache.local.protected = True

    tmp_dir.dvc_gen("foo", "foo")
    dvc.unprotect("foo")
    assert os.access("foo", os.W_OK)

    dvc.checkout(["foo.dvc"], relink=True)
    assert not os.access("foo", os.W_OK)
