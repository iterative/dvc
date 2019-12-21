from __future__ import unicode_literals

import filecmp
import logging
import os
import posixpath
import shutil
import time

import colorama
import pytest
from mock import patch

import dvc
from dvc.cache import Cache
from dvc.exceptions import DvcException
from dvc.exceptions import RecursiveAddingWhileUsingFilename
from dvc.exceptions import StageFileCorruptedError
from dvc.main import main
from dvc.output.base import OutputAlreadyTrackedError
from dvc.remote import RemoteLOCAL
from dvc.repo import Repo as DvcRepo
from dvc.stage import Stage
from dvc.system import System
from dvc.utils import file_md5
from dvc.utils import LARGE_DIR_SIZE
from dvc.utils import relpath
from dvc.utils.compat import range, fspath
from dvc.utils.fs import path_isin
from dvc.utils.stage import load_stage_file
from tests.basic_env import TestDvc
from tests.utils import get_gitignore_content
from tests.utils import spy


def test_add(tmp_dir, dvc):
    stage, = tmp_dir.dvc_gen({"foo": "foo"})
    md5, _ = file_md5("foo")

    assert stage is not None

    assert isinstance(stage, Stage)
    assert os.path.isfile(stage.path)
    assert len(stage.outs) == 1
    assert len(stage.deps) == 0
    assert stage.cmd is None
    assert stage.outs[0].info["md5"] == md5
    assert stage.md5 == "ee343f2482f53efffc109be83cc976ac"


def test_add_unicode(tmp_dir, dvc):
    with open("\xe1", "wb") as fd:
        fd.write("something".encode("utf-8"))

    stage, = dvc.add("\xe1")

    assert os.path.isfile(stage.path)


def test_add_unsupported_file(dvc):
    with pytest.raises(DvcException):
        dvc.add("unsupported://unsupported")


def test_add_directory(tmp_dir, dvc):
    stage, = tmp_dir.dvc_gen({"dir": {"file": "file"}})

    assert stage is not None
    assert len(stage.deps) == 0
    assert len(stage.outs) == 1

    md5 = stage.outs[0].info["md5"]

    dir_info = dvc.cache.local.load_dir_cache(md5)
    for info in dir_info:
        assert "\\" not in info["relpath"]


class TestAddDirectoryRecursive(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR, recursive=True)
        self.assertEqual(len(stages), 2)


class TestAddCmdDirectoryRecursive(TestDvc):
    def test(self):
        ret = main(["add", "--recursive", self.DATA_DIR])
        self.assertEqual(ret, 0)

    def test_warn_about_large_directories(self):
        warning = (
            "You are adding a large directory 'large-dir' recursively,"
            " consider tracking it as a whole instead.\n"
            "{purple}HINT:{nc} Remove the generated DVC-file and then"
            " run `{cyan}dvc add large-dir{nc}`".format(
                purple=colorama.Fore.MAGENTA,
                cyan=colorama.Fore.CYAN,
                nc=colorama.Style.RESET_ALL,
            )
        )

        os.mkdir("large-dir")

        # Create a lot of files
        for iteration in range(LARGE_DIR_SIZE + 1):
            path = os.path.join("large-dir", str(iteration))
            with open(path, "w") as fobj:
                fobj.write(path)

        with self._caplog.at_level(logging.WARNING, logger="dvc"):
            assert main(["add", "--recursive", "large-dir"]) == 0
            assert warning in self._caplog.messages


class TestAddDirectoryWithForwardSlash(TestDvc):
    def test(self):
        dname = "directory/"
        os.mkdir(dname)
        self.create(os.path.join(dname, "file"), "file")
        stages = self.dvc.add(dname)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)
        self.assertEqual(os.path.abspath("directory.dvc"), stage.path)


def test_add_tracked_file(tmp_dir, scm, dvc):
    tmp_dir.scm_gen("tracked_file", "...", commit="add tracked file")

    with pytest.raises(OutputAlreadyTrackedError):
        dvc.add("tracked_file")


class TestAddDirWithExistingCache(TestDvc):
    def test(self):
        dname = "a"
        fname = os.path.join(dname, "b")
        os.mkdir(dname)
        shutil.copyfile(self.FOO, fname)

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)
        stages = self.dvc.add(dname)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)


class TestAddModifiedDir(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)
        os.unlink(self.DATA)

        time.sleep(2)

        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)


def test_add_file_in_dir(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"subdir": {"subdata": "subdata content"}}})
    subdir_path = os.path.join("dir", "subdir", "subdata")

    stage, = dvc.add(subdir_path)

    assert stage is not None
    assert len(stage.deps) == 0
    assert len(stage.outs) == 1
    assert stage.relpath == subdir_path + ".dvc"

    # Current dir should not be taken into account
    assert stage.wdir == os.path.dirname(stage.path)
    assert stage.outs[0].def_path == "subdata"


class TestAddExternalLocalFile(TestDvc):
    def test(self):
        dname = TestDvc.mkdtemp()
        fname = os.path.join(dname, "foo")
        shutil.copyfile(self.FOO, fname)

        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertNotEqual(stage, None)
        self.assertEqual(len(stage.deps), 0)
        self.assertEqual(len(stage.outs), 1)
        self.assertEqual(stage.relpath, "foo.dvc")
        self.assertEqual(len(os.listdir(dname)), 1)
        self.assertTrue(os.path.isfile(fname))
        self.assertTrue(filecmp.cmp(fname, "foo", shallow=False))


class TestAddLocalRemoteFile(TestDvc):
    def test(self):
        """
        Making sure that 'remote' syntax is handled properly for local outs.
        """
        cwd = os.getcwd()
        remote = "myremote"

        ret = main(["remote", "add", remote, cwd])
        self.assertEqual(ret, 0)

        self.dvc = DvcRepo()

        foo = "remote://{}/{}".format(remote, self.FOO)
        ret = main(["add", foo])
        self.assertEqual(ret, 0)

        d = load_stage_file("foo.dvc")
        self.assertEqual(d["outs"][0]["path"], foo)

        bar = os.path.join(cwd, self.BAR)
        ret = main(["add", bar])
        self.assertEqual(ret, 0)

        d = load_stage_file("bar.dvc")
        self.assertEqual(d["outs"][0]["path"], bar)


class TestCmdAdd(TestDvc):
    def test(self):
        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["add", "non-existing-file"])
        self.assertNotEqual(ret, 0)


class TestDoubleAddUnchanged(TestDvc):
    def test_file(self):
        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

    def test_dir(self):
        ret = main(["add", self.DATA_DIR])
        self.assertEqual(ret, 0)

        ret = main(["add", self.DATA_DIR])
        self.assertEqual(ret, 0)


class TestShouldUpdateStateEntryForFileAfterAdd(TestDvc):
    def test(self):
        file_md5_counter = spy(dvc.remote.local.file_md5)
        with patch.object(dvc.remote.local, "file_md5", file_md5_counter):
            ret = main(["config", "cache.type", "copy"])
            self.assertEqual(ret, 0)

            ret = main(["add", self.FOO])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 1)

            ret = main(["status"])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 1)

            ret = main(["run", "-d", self.FOO, "echo foo"])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 1)

            os.rename(self.FOO, self.FOO + ".back")
            ret = main(["checkout"])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 1)

            ret = main(["status"])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 1)


class TestShouldUpdateStateEntryForDirectoryAfterAdd(TestDvc):
    def test(self):
        file_md5_counter = spy(dvc.remote.local.file_md5)
        with patch.object(dvc.remote.local, "file_md5", file_md5_counter):

            ret = main(["config", "cache.type", "copy"])
            self.assertEqual(ret, 0)

            ret = main(["add", self.DATA_DIR])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 3)

            ret = main(["status"])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 3)

            ls = "dir" if os.name == "nt" else "ls"
            ret = main(
                ["run", "-d", self.DATA_DIR, "{} {}".format(ls, self.DATA_DIR)]
            )
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 3)

            os.rename(self.DATA_DIR, self.DATA_DIR + ".back")
            ret = main(["checkout"])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 3)

            ret = main(["status"])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 3)


class TestAddCommit(TestDvc):
    def test(self):
        ret = main(["add", self.FOO, "--no-commit"])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.FOO))
        self.assertFalse(os.path.exists(self.dvc.cache.local.cache_dir))

        ret = main(["commit", self.FOO + ".dvc"])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.FOO))
        self.assertEqual(len(os.listdir(self.dvc.cache.local.cache_dir)), 1)


class TestShouldCollectDirCacheOnlyOnce(TestDvc):
    def test(self):
        from dvc.remote.local import RemoteLOCAL

        get_dir_checksum_counter = spy(RemoteLOCAL.get_dir_checksum)
        with patch.object(
            RemoteLOCAL, "get_dir_checksum", get_dir_checksum_counter
        ):
            ret = main(["add", self.DATA_DIR])
            self.assertEqual(0, ret)

            ret = main(["status"])
            self.assertEqual(0, ret)

            ret = main(["status"])
            self.assertEqual(0, ret)
        self.assertEqual(1, get_dir_checksum_counter.mock.call_count)


class SymlinkAddTestBase(TestDvc):
    def _get_data_dir(self):
        raise NotImplementedError

    def _prepare_external_data(self):
        data_dir = self._get_data_dir()

        self.data_file_name = "data_file"
        external_data_path = os.path.join(data_dir, self.data_file_name)
        with open(external_data_path, "w+") as f:
            f.write("data")

        self.link_name = "data_link"
        System.symlink(data_dir, self.link_name)

    def _test(self):
        self._prepare_external_data()

        ret = main(["add", os.path.join(self.link_name, self.data_file_name)])
        self.assertEqual(0, ret)

        stage_file = self.data_file_name + Stage.STAGE_FILE_SUFFIX
        self.assertTrue(os.path.exists(stage_file))

        d = load_stage_file(stage_file)
        relative_data_path = posixpath.join(
            self.link_name, self.data_file_name
        )
        self.assertEqual(relative_data_path, d["outs"][0]["path"])


class TestShouldAddDataFromExternalSymlink(SymlinkAddTestBase):
    def _get_data_dir(self):
        return self.mkdtemp()

    def test(self):
        self._test()


class TestShouldAddDataFromInternalSymlink(SymlinkAddTestBase):
    def _get_data_dir(self):
        return self.DATA_DIR

    def test(self):
        self._test()


class TestShouldPlaceStageInDataDirIfRepositoryBelowSymlink(TestDvc):
    def test(self):
        def is_symlink_true_below_dvc_root(path):
            if path == os.path.dirname(self.dvc.root_dir):
                return True
            return False

        with patch.object(
            System, "is_symlink", side_effect=is_symlink_true_below_dvc_root
        ):

            ret = main(["add", self.DATA])
            self.assertEqual(0, ret)

            stage_file_path_on_data_below_symlink = (
                os.path.basename(self.DATA) + Stage.STAGE_FILE_SUFFIX
            )
            self.assertFalse(
                os.path.exists(stage_file_path_on_data_below_symlink)
            )

            stage_file_path = self.DATA + Stage.STAGE_FILE_SUFFIX
            self.assertTrue(os.path.exists(stage_file_path))


class TestShouldThrowProperExceptionOnCorruptedStageFile(TestDvc):
    def test(self):
        ret = main(["add", self.FOO])
        assert 0 == ret

        foo_stage = relpath(self.FOO + Stage.STAGE_FILE_SUFFIX)

        # corrupt stage file
        with open(foo_stage, "a+") as file:
            file.write("this will break yaml file structure")

        self._caplog.clear()

        ret = main(["add", self.BAR])
        assert 1 == ret

        expected_error = (
            "unable to read DVC-file: {} "
            "YAML file structure is corrupted".format(foo_stage)
        )

        assert expected_error in self._caplog.text


class TestAddFilename(TestDvc):
    def test(self):
        ret = main(["add", self.FOO, self.BAR, "-f", "error.dvc"])
        self.assertNotEqual(0, ret)

        ret = main(["add", "-R", self.DATA_DIR, "-f", "error.dvc"])
        self.assertNotEqual(0, ret)

        with self.assertRaises(RecursiveAddingWhileUsingFilename):
            self.dvc.add(self.DATA_DIR, recursive=True, fname="error.dvc")

        ret = main(["add", self.DATA_DIR, "-f", "data_directory.dvc"])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists("data_directory.dvc"))

        ret = main(["add", self.FOO, "-f", "bar.dvc"])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists("bar.dvc"))
        self.assertFalse(os.path.exists("foo.dvc"))

        os.remove("bar.dvc")

        ret = main(["add", self.FOO, "--file", "bar.dvc"])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists("bar.dvc"))
        self.assertFalse(os.path.exists("foo.dvc"))


def test_should_cleanup_after_failed_add(tmp_dir, scm, dvc, repo_template):
    # Add and corrupt a stage file
    dvc.add("foo")
    tmp_dir.gen("foo.dvc", "- broken\nyaml")

    with pytest.raises(StageFileCorruptedError):
        dvc.add("bar")

    assert not os.path.exists("bar.dvc")

    gitignore_content = get_gitignore_content()
    assert "/bar" not in gitignore_content


class TestShouldNotTrackGitInternalFiles(TestDvc):
    def test(self):
        stage_creator_spy = spy(dvc.repo.add._create_stages)

        with patch.object(dvc.repo.add, "_create_stages", stage_creator_spy):
            ret = main(["add", "-R", self.dvc.root_dir])
            self.assertEqual(0, ret)

        created_stages_filenames = stage_creator_spy.mock.call_args[0][0]
        for fname in created_stages_filenames:
            self.assertNotIn(".git", fname)


class TestAddUnprotected(TestDvc):
    def test(self):
        ret = main(["config", "cache.type", "hardlink"])
        self.assertEqual(ret, 0)

        ret = main(["config", "cache.protected", "true"])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["unprotect", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        self.assertFalse(os.access(self.FOO, os.W_OK))
        self.assertTrue(System.is_hardlink(self.FOO))


@pytest.fixture
def temporary_windows_drive(tmp_path_factory):
    import string
    import win32api
    from ctypes import windll
    from win32con import DDD_REMOVE_DEFINITION

    drives = [
        s[0].upper()
        for s in win32api.GetLogicalDriveStrings().split("\000")
        if len(s) > 0
    ]

    new_drive_name = [
        letter for letter in string.ascii_uppercase if letter not in drives
    ][0]
    new_drive = "{}:".format(new_drive_name)

    target_path = tmp_path_factory.mktemp("tmp_windows_drive")

    set_up_result = windll.kernel32.DefineDosDeviceW(
        0, new_drive, fspath(target_path)
    )
    if set_up_result == 0:
        raise RuntimeError("Failed to mount windows drive!")

    # NOTE: new_drive has form of `A:` and joining it with some relative
    # path might result in non-existing path (A:path\\to)
    yield os.path.join(new_drive, os.sep)

    tear_down_result = windll.kernel32.DefineDosDeviceW(
        DDD_REMOVE_DEFINITION, new_drive, fspath(target_path)
    )
    if tear_down_result == 0:
        raise RuntimeError("Could not unmount windows drive!")


@pytest.mark.skipif(os.name != "nt", reason="Windows specific")
def test_windows_should_add_when_cache_on_different_drive(
    tmp_dir, dvc, temporary_windows_drive
):
    dvc.config.set("cache", "dir", temporary_windows_drive)
    dvc.cache = Cache(dvc)

    stage, = tmp_dir.dvc_gen({"file": "file"})
    cache_path = stage.outs[0].cache_path

    assert path_isin(cache_path, temporary_windows_drive)
    assert os.path.isfile(cache_path)
    filecmp.cmp("file", cache_path)


def test_readding_dir_should_not_unprotect_all(tmp_dir, dvc, mocker):
    tmp_dir.gen("dir/data", "data")

    dvc.cache.local.cache_types = ["symlink"]
    dvc.cache.local.protected = True

    dvc.add("dir")
    tmp_dir.gen("dir/new_file", "new_file_content")

    unprotect_spy = spy(RemoteLOCAL.unprotect)
    mocker.patch.object(RemoteLOCAL, "unprotect", unprotect_spy)
    dvc.add("dir")

    assert not unprotect_spy.mock.called
    assert System.is_symlink(os.path.join("dir", "new_file"))


def test_should_not_checkout_when_adding_cached_copy(tmp_dir, dvc, mocker):
    dvc.cache.local.cache_types = ["copy"]

    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})

    shutil.copy("bar", "foo")

    copy_spy = spy(dvc.cache.local.copy)
    mocker.patch.object(dvc.cache.local, "copy", copy_spy)

    dvc.add("foo")

    assert copy_spy.mock.call_count == 0


@pytest.mark.parametrize(
    "link,new_link,link_test_func",
    [
        ("hardlink", "copy", lambda path: not System.is_hardlink(path)),
        ("symlink", "copy", lambda path: not System.is_symlink(path)),
        ("copy", "hardlink", System.is_hardlink),
        ("copy", "symlink", System.is_symlink),
    ],
)
def test_should_relink_on_repeated_add(
    link, new_link, link_test_func, tmp_dir, dvc
):
    from dvc.path_info import PathInfo

    dvc.config.set("cache", "type", link)

    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})

    os.remove("foo")
    getattr(dvc.cache.local, link)(PathInfo("bar"), PathInfo("foo"))

    dvc.cache.local.cache_types = [new_link]

    dvc.add("foo")

    assert link_test_func("foo")


@pytest.mark.parametrize("link", ["hardlink", "symlink", "copy"])
def test_should_protect_on_repeated_add(link, tmp_dir, dvc):
    dvc.cache.local.cache_types = [link]
    dvc.cache.local.protected = True

    tmp_dir.dvc_gen({"foo": "foo"})

    dvc.unprotect("foo")

    dvc.add("foo")

    assert not os.access("foo", os.W_OK)


def test_escape_gitignore_entries(tmp_dir, scm, dvc):
    fname = "file!with*weird#naming_[1].t?t"
    ignored_fname = r"/file\!with\*weird\#naming_\[1\].t\?t"

    if os.name == "nt":
        # Some characters are not supported by Windows in the filename
        # https://docs.microsoft.com/en-us/windows/win32/fileio/naming-a-file
        fname = "file!with_weird#naming_[1].txt"
        ignored_fname = r"/file\!with_weird\#naming_\[1\].txt"

    tmp_dir.dvc_gen(fname, "...")
    assert ignored_fname in get_gitignore_content()
