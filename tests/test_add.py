from __future__ import unicode_literals

import os

import dvc
import yaml
import time
import shutil
import filecmp

from mock import patch

from dvc.main import main
from dvc.utils import file_md5
from dvc.stage import Stage
from dvc.exceptions import DvcException
from dvc.output.base import OutputAlreadyTrackedError
from dvc.repo import Repo as DvcRepo

from tests.basic_env import TestDvc
from tests.utils import spy


class TestAdd(TestDvc):
    def test(self):
        md5 = file_md5(self.FOO)[0]

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)

        self.assertIsInstance(stage, Stage)
        self.assertTrue(os.path.isfile(stage.path))
        self.assertEqual(len(stage.outs), 1)
        self.assertEqual(len(stage.deps), 0)
        self.assertEqual(stage.cmd, None)
        self.assertEqual(stage.outs[0].info["md5"], md5)

    def test_unicode(self):
        fname = "\xe1"

        with open(fname, "w") as fobj:
            fobj.write("something")

        stage = self.dvc.add(fname)[0]

        self.assertTrue(os.path.isfile(stage.path))


class TestAddUnupportedFile(TestDvc):
    def test(self):
        with self.assertRaises(DvcException):
            self.dvc.add("unsupported://unsupported")


class TestAddDirectory(TestDvc):
    def test(self):
        dname = "directory"
        os.mkdir(dname)
        self.create(os.path.join(dname, "file"), "file")
        stages = self.dvc.add(dname)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)
        self.assertEqual(len(stage.deps), 0)
        self.assertEqual(len(stage.outs), 1)

        md5 = stage.outs[0].info["md5"]

        dir_info = self.dvc.cache.local.load_dir_cache(md5)
        for info in dir_info:
            self.assertTrue("\\" not in info["relpath"])


class TestAddDirectoryRecursive(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR, recursive=True)
        self.assertEqual(len(stages), 2)


class TestAddCmdDirectoryRecursive(TestDvc):
    def test(self):
        ret = main(["add", "--recursive", self.DATA_DIR])
        self.assertEqual(ret, 0)


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


class TestAddTrackedFile(TestDvc):
    def test(self):
        fname = "tracked_file"
        self.create(fname, "tracked file contents")
        self.dvc.scm.add([fname])
        self.dvc.scm.commit("add {}".format(fname))

        with self.assertRaises(OutputAlreadyTrackedError):
            self.dvc.add(fname)


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


class TestAddFileInDir(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_SUB)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertNotEqual(stage, None)
        self.assertEqual(len(stage.deps), 0)
        self.assertEqual(len(stage.outs), 1)
        self.assertEqual(stage.relpath, self.DATA_SUB + ".dvc")


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

        with open("foo.dvc", "r") as fobj:
            d = yaml.safe_load(fobj)
            self.assertEqual(d["outs"][0]["path"], foo)

        bar = os.path.join(cwd, self.BAR)
        ret = main(["add", bar])
        self.assertEqual(ret, 0)

        with open("bar.dvc", "r") as fobj:
            d = yaml.safe_load(fobj)
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
        file_md5_counter = spy(dvc.state.file_md5)
        with patch.object(dvc.state, "file_md5", file_md5_counter):

            ret = main(["config", "cache.type", "copy"])
            self.assertEqual(ret, 0)

            ret = main(["add", self.FOO])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 1)

            ret = main(["status"])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 1)

            ret = main(["run", "-d", self.FOO, "cat {}".format(self.FOO)])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 1)


class TestShouldUpdateStateEntryForDirectoryAfterAdd(TestDvc):
    def test(self):
        file_md5_counter = spy(dvc.state.file_md5)
        with patch.object(dvc.state, "file_md5", file_md5_counter):

            ret = main(["config", "cache.type", "copy"])
            self.assertEqual(ret, 0)

            ret = main(["add", self.DATA_DIR])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 3)

            ret = main(["status"])
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 3)

            ret = main(
                ["run", "-d", self.DATA_DIR, "ls {}".format(self.DATA_DIR)]
            )
            self.assertEqual(ret, 0)
            self.assertEqual(file_md5_counter.mock.call_count, 3)


class TestAddCommit(TestDvc):
    def test(self):
        ret = main(["add", self.FOO, "--no-commit"])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.FOO))
        self.assertEqual(len(os.listdir(self.dvc.cache.local.cache_dir)), 0)

        ret = main(["commit", self.FOO + ".dvc"])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.FOO))
        self.assertEqual(len(os.listdir(self.dvc.cache.local.cache_dir)), 1)


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
            self.assertEqual(ret, 0)

            ret = main(["add", self.DATA_DIR])
            self.assertEqual(ret, 0)
            self.assertEqual(1, remote_local_loader_spy.mock.call_count)

            ret = main(["status", "{}.dvc".format(self.DATA_DIR)])
            self.assertEqual(ret, 0)
            self.assertEqual(1, remote_local_loader_spy.mock.call_count)


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
            self.assertEqual(0, ret)

            ret = main(["status"])
            self.assertEqual(0, ret)

            ret = main(["status"])
            self.assertEqual(0, ret)
        self.assertEqual(1, collect_dir_counter.mock.call_count)
