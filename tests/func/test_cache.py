import logging
import os
import stat

import configobj
import pytest

from dvc.cache import Cache
from dvc.cache.base import DirCacheError
from dvc.hash_info import HashInfo
from dvc.main import main
from dvc.utils import relpath
from tests.basic_env import TestDir, TestDvc, TestDvcFixture


class TestCache(TestDvc):
    def setUp(self):
        super().setUp()
        self.cache1_md5 = "123"
        self.cache2_md5 = "234"
        self.cache1 = os.path.join(
            self.dvc.cache.local.cache_dir,
            self.cache1_md5[0:2],
            self.cache1_md5[2:],
        )
        self.cache2 = os.path.join(
            self.dvc.cache.local.cache_dir,
            self.cache2_md5[0:2],
            self.cache2_md5[2:],
        )
        self.create(self.cache1, "1")
        self.create(self.cache2, "2")

    def test_all(self):
        md5_list = list(Cache(self.dvc).local.tree.all())
        self.assertEqual(len(md5_list), 2)
        self.assertIn(self.cache1_md5, md5_list)
        self.assertIn(self.cache2_md5, md5_list)

    def test_get(self):
        cache = Cache(self.dvc).local.tree.hash_to_path_info(self.cache1_md5)
        self.assertEqual(os.fspath(cache), self.cache1)


class TestCacheLoadBadDirCache(TestDvc):
    def _do_test(self, ret):
        self.assertTrue(isinstance(ret, list))
        self.assertEqual(len(ret), 0)

    def test(self):
        dir_hash = "123.dir"
        fname = os.fspath(
            self.dvc.cache.local.tree.hash_to_path_info(dir_hash)
        )
        self.create(fname, "<clearly>not,json")
        with pytest.raises(DirCacheError):
            self.dvc.cache.local.load_dir_cache(HashInfo("md5", dir_hash))

        dir_hash = "234.dir"
        fname = os.fspath(
            self.dvc.cache.local.tree.hash_to_path_info(dir_hash)
        )
        self.create(fname, '{"a": "b"}')
        self._do_test(
            self.dvc.cache.local.load_dir_cache(HashInfo("md5", dir_hash))
        )


class TestExternalCacheDir(TestDvc):
    def test(self):
        cache_dir = TestDvc.mkdtemp()

        ret = main(["config", "cache.dir", cache_dir])
        self.assertEqual(ret, 0)

        self.assertFalse(os.path.exists(self.dvc.cache.local.cache_dir))

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["add", self.DATA_DIR])
        self.assertEqual(ret, 0)

        self.assertFalse(os.path.exists(".dvc/cache"))
        self.assertNotEqual(len(os.listdir(cache_dir)), 0)

    def test_remote_references(self):
        ssh_url = "ssh://user@localhost:23"
        assert main(["remote", "add", "storage", ssh_url]) == 0
        assert main(["remote", "add", "cache", "remote://storage/tmp"]) == 0
        assert main(["config", "cache.ssh", "cache"]) == 0

        self.dvc.__init__()

        assert self.dvc.cache.ssh.tree.path_info == ssh_url + "/tmp"


class TestSharedCacheDir(TestDir):
    def test(self):
        cache_dir = os.path.abspath(os.path.join(os.curdir, "cache"))
        for d in ["dir1", "dir2"]:
            os.mkdir(d)
            os.chdir(d)

            ret = main(["init", "--no-scm"])
            self.assertEqual(ret, 0)

            ret = main(["config", "cache.dir", cache_dir])
            self.assertEqual(ret, 0)

            self.assertFalse(os.path.exists(os.path.join(".dvc", "cache")))

            with open("common", "w+") as fd:
                fd.write("common")

            with open("unique", "w+") as fd:
                fd.write(d)

            ret = main(["add", "common", "unique"])
            self.assertEqual(ret, 0)

            os.chdir("..")

        self.assertFalse(os.path.exists(os.path.join("dir1", ".dvc", "cache")))
        self.assertFalse(os.path.exists(os.path.join("dir2", ".dvc", "cache")))

        subdirs = list(
            filter(
                lambda x: os.path.isdir(os.path.join(cache_dir, x)),
                os.listdir(cache_dir),
            )
        )
        self.assertEqual(len(subdirs), 3)
        self.assertEqual(
            len(os.listdir(os.path.join(cache_dir, subdirs[0]))), 1
        )
        self.assertEqual(
            len(os.listdir(os.path.join(cache_dir, subdirs[1]))), 1
        )
        self.assertEqual(
            len(os.listdir(os.path.join(cache_dir, subdirs[2]))), 1
        )


class TestCacheLinkType(TestDvc):
    def test(self):
        ret = main(["config", "cache.type", "reflink,copy"])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)


class TestCmdCacheDir(TestDvc):
    def test(self):
        ret = main(["cache", "dir"])
        self.assertEqual(ret, 0)

    def test_abs_path(self):
        dname = os.path.join(os.path.dirname(self._root_dir), "dir")
        ret = main(["cache", "dir", dname])
        self.assertEqual(ret, 0)

        config = configobj.ConfigObj(self.dvc.config.files["repo"])
        self.assertEqual(config["cache"]["dir"], dname.replace("\\", "/"))

    def test_relative_path(self):
        tmpdir = os.path.realpath(self.mkdtemp())
        dname = relpath(tmpdir)
        ret = main(["cache", "dir", dname])
        self.assertEqual(ret, 0)

        # NOTE: we are in the repo's root and config is in .dvc/, so
        # dir path written to config should be just one level above.
        rel = os.path.join("..", dname)
        config = configobj.ConfigObj(self.dvc.config.files["repo"])
        self.assertEqual(config["cache"]["dir"], rel.replace("\\", "/"))

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        subdirs = os.listdir(tmpdir)
        self.assertEqual(len(subdirs), 1)
        files = os.listdir(os.path.join(tmpdir, subdirs[0]))
        self.assertEqual(len(files), 1)


def test_cache_status_command(caplog):
    dvc_fixture = TestDvcFixture()
    dvc_fixture.setUp()
    dvc_fixture.dvc.add(TestDvcFixture.FOO)
    dvc_fixture.dvc.add(TestDvcFixture.DATA_DIR, recursive=True)
    dvc_fixture.dvc.add(TestDvcFixture.DATA_SUB_DIR, recursive=True)

    with caplog.at_level(logging.INFO):
        ret = main(["cache", "status"])
        assert ret == 0
        logs = "\n".join([record.message for record in caplog.records])
        assert "Step 1: Permission Check on:" in logs
        assert dvc_fixture.root_dir in logs
        assert "Read: OK" in logs
        assert "Write: OK" in logs
        assert "Exist: OK" in logs
        assert "Step 2: DVC Files and Cache status." in logs
        assert TestDvcFixture.FOO in logs
        assert ".dvc/cache/ac/bd18db4cc2f85cedef654fccc4a4d8" in logs
        assert TestDvcFixture.DATA_SUB_DIR in logs
        assert ".dvc/cache/1a/d1c255771ec00b7cee20a136250065" in logs
        assert TestDvcFixture.DATA_SUB in logs
        assert ".dvc/cache/2b/7235bae9a59ef5602ad01d5719aabc" in logs


def test_default_cache_type(dvc):
    assert dvc.cache.local.cache_types == ["reflink", "copy"]


@pytest.mark.skipif(os.name == "nt", reason="Not supported for Windows.")
@pytest.mark.parametrize(
    "group, dir_mode", [(False, 0o755), (True, 0o775)],
)
def test_shared_cache(tmp_dir, dvc, group, dir_mode):
    if group:
        with dvc.config.edit() as conf:
            conf["cache"].update({"shared": "group"})
    dvc.cache = Cache(dvc)

    tmp_dir.dvc_gen(
        {"file": "file content", "dir": {"file2": "file 2 " "content"}}
    )

    for root, dnames, fnames in os.walk(dvc.cache.local.cache_dir):
        for dname in dnames:
            path = os.path.join(root, dname)
            assert stat.S_IMODE(os.stat(path).st_mode) == dir_mode

        for fname in fnames:
            path = os.path.join(root, fname)
            assert stat.S_IMODE(os.stat(path).st_mode) == 0o444
