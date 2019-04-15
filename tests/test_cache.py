import os
import shutil
import configobj

from dvc.cache import Cache
from dvc.main import main

from tests.basic_env import TestDvc, TestDir


class TestCache(TestDvc):
    def setUp(self):
        super(TestCache, self).setUp()
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
        md5_list = list(Cache(self.dvc).local.all())
        self.assertEqual(len(md5_list), 2)
        self.assertIn(self.cache1_md5, md5_list)
        self.assertIn(self.cache2_md5, md5_list)

    def test_get(self):
        cache = Cache(self.dvc).local.get(self.cache1_md5)
        self.assertEqual(cache, self.cache1)


class TestCacheLoadBadDirCache(TestDvc):
    def _do_test(self, ret):
        self.assertTrue(isinstance(ret, list))
        self.assertEqual(len(ret), 0)

    def test(self):
        fname = self.dvc.cache.local.get("123.dir")
        self.create(fname, "<clearly>not,json")
        self._do_test(self.dvc.cache.local.load_dir_cache(fname))

        fname = self.dvc.cache.local.get("234.dir")
        self.create(fname, '{"a": "b"}')
        self._do_test(self.dvc.cache.local.load_dir_cache(fname))


class TestExternalCacheDir(TestDvc):
    def test(self):
        cache_dir = TestDvc.mkdtemp()

        ret = main(["config", "cache.dir", cache_dir])
        self.assertEqual(ret, 0)

        shutil.rmtree(".dvc/cache")

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["add", self.DATA_DIR])
        self.assertEqual(ret, 0)

        self.assertFalse(os.path.exists(".dvc/cache"))
        self.assertNotEqual(len(os.listdir(cache_dir)), 0)

    def test_remote_references(self):
        assert main(["remote", "add", "storage", "ssh://localhost"]) == 0
        assert main(["remote", "add", "cache", "remote://storage/tmp"]) == 0
        assert main(["config", "cache.ssh", "cache"]) == 0

        self.dvc.__init__()

        assert self.dvc.cache.ssh.url == "ssh://localhost/tmp"


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

            shutil.rmtree(os.path.join(".dvc", "cache"))

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
        self.assertEqual(ret, 254)

    def test_abs_path(self):
        dname = os.path.join(os.path.dirname(self._root_dir), "dir")
        ret = main(["cache", "dir", dname])
        self.assertEqual(ret, 0)

        config = configobj.ConfigObj(self.dvc.config.config_file)
        self.assertEqual(config["cache"]["dir"], dname)

    def test_relative_path(self):
        tmpdir = self.mkdtemp()
        dname = os.path.relpath(tmpdir)
        ret = main(["cache", "dir", dname])
        self.assertEqual(ret, 0)

        # NOTE: we are in the repo's root and config is in .dvc/, so
        # dir path written to config should be just one level above.
        rel = os.path.join("..", dname)
        config = configobj.ConfigObj(self.dvc.config.config_file)
        self.assertEqual(config["cache"]["dir"], rel)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        subdirs = os.listdir(tmpdir)
        self.assertEqual(len(subdirs), 1)
        files = os.listdir(os.path.join(tmpdir, subdirs[0]))
        self.assertEqual(len(files), 1)


class TestShouldCacheBeReflinkOrCopyByDefault(TestDvc):
    def test(self):
        self.assertEqual(self.dvc.cache.local.cache_types, ["reflink", "copy"])
