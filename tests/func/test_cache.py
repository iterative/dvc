import os
import stat

import configobj
import pytest

from dvc.cache import Cache
from dvc.main import main
from dvc.remote.base import DirCacheError
from dvc.utils import relpath
from tests.basic_env import TestDir
from tests.basic_env import TestDvc


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
        checksum = "123.dir"
        fname = self.dvc.cache.local.get(checksum)
        self.create(fname, "<clearly>not,json")
        with pytest.raises(DirCacheError):
            self.dvc.cache.local.load_dir_cache(checksum)

        checksum = "234.dir"
        fname = self.dvc.cache.local.get(checksum)
        self.create(fname, '{"a": "b"}')
        self._do_test(self.dvc.cache.local.load_dir_cache(checksum))


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

        assert self.dvc.cache.ssh.path_info == ssh_url + "/tmp"


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
        self.assertEqual(ret, 254)

    def test_abs_path(self):
        dname = os.path.join(os.path.dirname(self._root_dir), "dir")
        ret = main(["cache", "dir", dname])
        self.assertEqual(ret, 0)

        config = configobj.ConfigObj(self.dvc.config.config_file)
        self.assertEqual(config["cache"]["dir"], dname)

    def test_relative_path(self):
        tmpdir = self.mkdtemp()
        dname = relpath(tmpdir)
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


@pytest.mark.skipif(os.name == "nt", reason="Not supported for Windows.")
@pytest.mark.parametrize(
    "protected,dir_mode,file_mode",
    [(False, 0o775, 0o664), (True, 0o775, 0o444)],
)
def test_shared_cache(repo_dir, dvc_repo, protected, dir_mode, file_mode):
    assert main(["config", "cache.shared", "group"]) == 0

    if protected:
        assert main(["config", "cache.protected", "true"]) == 0

    assert main(["add", repo_dir.FOO]) == 0
    assert main(["add", repo_dir.DATA_DIR]) == 0

    for root, dnames, fnames in os.walk(dvc_repo.cache.local.cache_dir):
        for dname in dnames:
            path = os.path.join(root, dname)
            assert stat.S_IMODE(os.stat(path).st_mode) == dir_mode

        for fname in fnames:
            path = os.path.join(root, fname)
            assert stat.S_IMODE(os.stat(path).st_mode) == file_mode
