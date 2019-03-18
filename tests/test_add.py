from __future__ import unicode_literals

import os

import dvc
import time
import shutil
import filecmp
import posixpath

from dvc.logger import logger
from dvc.system import System
from mock import patch

from dvc.main import main
from dvc.utils import file_md5, load_stage_file
from dvc.stage import Stage
from dvc.exceptions import DvcException, RecursiveAddingWhileUsingFilename
from dvc.output.base import OutputAlreadyTrackedError
from dvc.repo import Repo as DvcRepo

from tests.basic_env import TestDvc
from tests.utils import spy, reset_logger_error_output, get_gitignore_content
from tests.utils.logger import MockLoggerHandlers, ConsoleFontColorsRemover
import pytest


class TestAdd(TestDvc):
    def test(self):
        md5 = file_md5(self.FOO)[0]

        stages = self.dvc.add(self.FOO)
        assert len(stages) == 1
        stage = stages[0]
        assert stage is not None

        assert isinstance(stage, Stage)
        assert os.path.isfile(stage.path)
        assert len(stage.outs) == 1
        assert len(stage.deps) == 0
        assert stage.cmd is None
        assert stage.outs[0].info["md5"] == md5

    def test_unicode(self):
        fname = "\xe1"

        with open(fname, "w") as fobj:
            fobj.write("something")

        stage = self.dvc.add(fname)[0]

        assert os.path.isfile(stage.path)


class TestAddUnupportedFile(TestDvc):
    def test(self):
        with pytest.raises(DvcException):
            self.dvc.add("unsupported://unsupported")


class TestAddDirectory(TestDvc):
    def test(self):
        dname = "directory"
        os.mkdir(dname)
        self.create(os.path.join(dname, "file"), "file")
        stages = self.dvc.add(dname)
        assert len(stages) == 1
        stage = stages[0]
        assert stage is not None
        assert len(stage.deps) == 0
        assert len(stage.outs) == 1

        md5 = stage.outs[0].info["md5"]

        dir_info = self.dvc.cache.local.load_dir_cache(md5)
        for info in dir_info:
            assert "\\" not in info["relpath"]


class TestAddDirectoryRecursive(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR, recursive=True)
        assert len(stages) == 2


class TestAddCmdDirectoryRecursive(TestDvc):
    def test(self):
        ret = main(["add", "--recursive", self.DATA_DIR])
        assert ret == 0


class TestAddDirectoryWithForwardSlash(TestDvc):
    def test(self):
        dname = "directory/"
        os.mkdir(dname)
        self.create(os.path.join(dname, "file"), "file")
        stages = self.dvc.add(dname)
        assert len(stages) == 1
        stage = stages[0]
        assert stage is not None
        assert os.path.abspath("directory.dvc") == stage.path


class TestAddTrackedFile(TestDvc):
    def test(self):
        fname = "tracked_file"
        self.create(fname, "tracked file contents")
        self.dvc.scm.add([fname])
        self.dvc.scm.commit("add {}".format(fname))

        with pytest.raises(OutputAlreadyTrackedError):
            self.dvc.add(fname)


class TestAddDirWithExistingCache(TestDvc):
    def test(self):
        dname = "a"
        fname = os.path.join(dname, "b")
        os.mkdir(dname)
        shutil.copyfile(self.FOO, fname)

        stages = self.dvc.add(self.FOO)
        assert len(stages) == 1
        assert stages[0] is not None
        stages = self.dvc.add(dname)
        assert len(stages) == 1
        assert stages[0] is not None


class TestAddModifiedDir(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR)
        assert len(stages) == 1
        assert stages[0] is not None
        os.unlink(self.DATA)

        time.sleep(2)

        stages = self.dvc.add(self.DATA_DIR)
        assert len(stages) == 1
        assert stages[0] is not None


class TestAddFileInDir(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_SUB)
        assert len(stages) == 1
        stage = stages[0]
        assert stage is not None
        assert len(stage.deps) == 0
        assert len(stage.outs) == 1
        assert stage.relpath == self.DATA_SUB + ".dvc"


class TestAddExternalLocalFile(TestDvc):
    def test(self):
        dname = TestDvc.mkdtemp()
        fname = os.path.join(dname, "foo")
        shutil.copyfile(self.FOO, fname)

        stages = self.dvc.add(fname)
        assert len(stages) == 1
        stage = stages[0]
        assert stage is not None
        assert len(stage.deps) == 0
        assert len(stage.outs) == 1
        assert stage.relpath == "foo.dvc"
        assert len(os.listdir(dname)) == 1
        assert os.path.isfile(fname)
        assert filecmp.cmp(fname, "foo", shallow=False)


class TestAddLocalRemoteFile(TestDvc):
    def test(self):
        """
        Making sure that 'remote' syntax is handled properly for local outs.
        """
        cwd = os.getcwd()
        remote = "myremote"

        ret = main(["remote", "add", remote, cwd])
        assert ret == 0

        self.dvc = DvcRepo()

        foo = "remote://{}/{}".format(remote, self.FOO)
        ret = main(["add", foo])
        assert ret == 0

        d = load_stage_file("foo.dvc")
        assert d["outs"][0]["path"] == foo

        bar = os.path.join(cwd, self.BAR)
        ret = main(["add", bar])
        assert ret == 0

        d = load_stage_file("bar.dvc")
        assert d["outs"][0]["path"] == bar


class TestCmdAdd(TestDvc):
    def test(self):
        ret = main(["add", self.FOO])
        assert ret == 0

        ret = main(["add", "non-existing-file"])
        assert ret != 0


class TestDoubleAddUnchanged(TestDvc):
    def test_file(self):
        ret = main(["add", self.FOO])
        assert ret == 0

        ret = main(["add", self.FOO])
        assert ret == 0

    def test_dir(self):
        ret = main(["add", self.DATA_DIR])
        assert ret == 0

        ret = main(["add", self.DATA_DIR])
        assert ret == 0


class TestShouldUpdateStateEntryForFileAfterAdd(TestDvc):
    def test(self):
        file_md5_counter = spy(dvc.state.file_md5)
        with patch.object(dvc.state, "file_md5", file_md5_counter):

            ret = main(["config", "cache.type", "copy"])
            assert ret == 0

            ret = main(["add", self.FOO])
            assert ret == 0
            assert file_md5_counter.mock.call_count == 1

            ret = main(["status"])
            assert ret == 0
            assert file_md5_counter.mock.call_count == 1

            ret = main(["run", "-d", self.FOO, "cat {}".format(self.FOO)])
            assert ret == 0
            assert file_md5_counter.mock.call_count == 1


class TestShouldUpdateStateEntryForDirectoryAfterAdd(TestDvc):
    def test(self):
        file_md5_counter = spy(dvc.state.file_md5)
        with patch.object(dvc.state, "file_md5", file_md5_counter):

            ret = main(["config", "cache.type", "copy"])
            assert ret == 0

            ret = main(["add", self.DATA_DIR])
            assert ret == 0
            assert file_md5_counter.mock.call_count == 3

            ret = main(["status"])
            assert ret == 0
            assert file_md5_counter.mock.call_count == 3

            ret = main(
                ["run", "-d", self.DATA_DIR, "ls {}".format(self.DATA_DIR)]
            )
            assert ret == 0
            assert file_md5_counter.mock.call_count == 3


class TestAddCommit(TestDvc):
    def test(self):
        ret = main(["add", self.FOO, "--no-commit"])
        assert ret == 0
        assert os.path.isfile(self.FOO)
        assert len(os.listdir(self.dvc.cache.local.cache_dir)) == 0

        ret = main(["commit", self.FOO + ".dvc"])
        assert ret == 0
        assert os.path.isfile(self.FOO)
        assert len(os.listdir(self.dvc.cache.local.cache_dir)) == 1


class TestShouldNotCheckCacheForDirIfCacheMetadataDidNotChange(TestDvc):
    def test(self):
        remote_local_loader_spy = spy(
            dvc.remote.local.RemoteLOCAL.load_dir_cache
        )
        with patch.object(
            dvc.remote.local.RemoteLOCAL,
            "load_dir_cache",
            remote_local_loader_spy,
        ):

            ret = main(["config", "cache.type", "copy"])
            assert ret == 0

            ret = main(["add", self.DATA_DIR])
            assert ret == 0
            assert 1 == remote_local_loader_spy.mock.call_count

            ret = main(["status", "{}.dvc".format(self.DATA_DIR)])
            assert ret == 0
            assert 1 == remote_local_loader_spy.mock.call_count


class TestShouldCollectDirCacheOnlyOnce(TestDvc):
    NEW_LARGE_DIR_SIZE = 1

    @patch("dvc.remote.local.LARGE_DIR_SIZE", NEW_LARGE_DIR_SIZE)
    def test(self):
        from dvc.remote.local import RemoteLOCAL

        collect_dir_counter = spy(RemoteLOCAL.collect_dir_cache)
        with patch.object(
            RemoteLOCAL, "collect_dir_cache", collect_dir_counter
        ):

            LARGE_DIR_FILES_NUM = self.NEW_LARGE_DIR_SIZE + 1
            data_dir = "dir"

            os.makedirs(data_dir)

            for i in range(LARGE_DIR_FILES_NUM):
                with open(os.path.join(data_dir, str(i)), "w+") as f:
                    f.write(str(i))

            ret = main(["add", data_dir])
            assert 0 == ret

            ret = main(["status"])
            assert 0 == ret

            ret = main(["status"])
            assert 0 == ret
        assert 1 == collect_dir_counter.mock.call_count


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
        assert 0 == ret

        stage_file = self.data_file_name + Stage.STAGE_FILE_SUFFIX
        assert os.path.exists(stage_file)

        d = load_stage_file(stage_file)
        relative_data_path = posixpath.join(
            self.link_name, self.data_file_name
        )
        assert relative_data_path == d["outs"][0]["path"]


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
            assert 0 == ret

            stage_file_path_on_data_below_symlink = (
                os.path.basename(self.DATA) + Stage.STAGE_FILE_SUFFIX
            )
            assert not os.path.exists(stage_file_path_on_data_below_symlink)

            stage_file_path = self.DATA + Stage.STAGE_FILE_SUFFIX
            assert os.path.exists(stage_file_path)


class TestShouldThrowProperExceptionOnCorruptedStageFile(TestDvc):
    def test(self):
        with MockLoggerHandlers(logger), ConsoleFontColorsRemover():
            reset_logger_error_output()

            ret = main(["add", self.FOO])
            assert 0 == ret

            foo_stage = os.path.relpath(self.FOO + Stage.STAGE_FILE_SUFFIX)

            # corrupt stage file
            with open(foo_stage, "a+") as file:
                file.write("this will break yaml file structure")

            ret = main(["add", self.BAR])
            assert 1 == ret

            assert (
                "unable to read stage file: {} "
                "YAML file structure is corrupted".format(foo_stage)
                in logger.handlers[1].stream.getvalue()
            )


class TestAddFilename(TestDvc):
    def test(self):
        ret = main(["add", self.FOO, self.BAR, "-f", "error.dvc"])
        assert 0 != ret

        ret = main(["add", "-R", self.DATA_DIR, "-f", "error.dvc"])
        assert 0 != ret

        with pytest.raises(RecursiveAddingWhileUsingFilename):
            self.dvc.add(self.DATA_DIR, recursive=True, fname="error.dvc")

        ret = main(["add", self.DATA_DIR, "-f", "data_directory.dvc"])
        assert 0 == ret
        assert os.path.exists("data_directory.dvc")

        ret = main(["add", self.FOO, "-f", "bar.dvc"])
        assert 0 == ret
        assert os.path.exists("bar.dvc")
        assert not os.path.exists("foo.dvc")

        os.remove("bar.dvc")

        ret = main(["add", self.FOO, "--file", "bar.dvc"])
        assert 0 == ret
        assert os.path.exists("bar.dvc")
        assert not os.path.exists("foo.dvc")


class TestShouldCleanUpAfterFailedAdd(TestDvc):
    def test(self):
        ret = main(["add", self.FOO])
        assert 0 == ret

        foo_stage_file = self.FOO + Stage.STAGE_FILE_SUFFIX
        # corrupt stage file
        with open(foo_stage_file, "a+") as file:
            file.write("this will break yaml file structure")

        ret = main(["add", self.BAR])
        assert 1 == ret

        bar_stage_file = self.BAR + Stage.STAGE_FILE_SUFFIX
        assert not os.path.exists(bar_stage_file)

        gitignore_content = get_gitignore_content()
        assert not any(self.BAR in line for line in gitignore_content)


class TestShouldNotTrackGitInternalFiles(TestDvc):
    def test(self):
        stage_creator_spy = spy(dvc.repo.add._create_stages)

        with patch.object(dvc.repo.add, "_create_stages", stage_creator_spy):
            ret = main(["add", "-R", self.dvc.root_dir])
            assert 0 == ret

        created_stages_filenames = stage_creator_spy.mock.call_args[0][0]
        for fname in created_stages_filenames:
            assert ".git" not in fname
