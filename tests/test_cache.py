import os
import shutil
import tempfile

from dvc.cache import Cache
from dvc.system import System
from dvc.main import main

from tests.basic_env import TestDvc


class TestCache(TestDvc):
    def setUp(self):
        super(TestCache, self).setUp()
        self.cache1_md5 = '123'
        self.cache2_md5 = '234'
        self.cache1 = os.path.join(self.dvc.cache.cache_dir, self.cache1_md5[0:2], self.cache1_md5[2:])
        self.cache2 = os.path.join(self.dvc.cache.cache_dir, self.cache2_md5[0:2], self.cache2_md5[2:])
        self.create(self.cache1, '1')
        self.create(self.cache2, '2')

    def test_all(self):
        flist = Cache(self.dvc.root_dir, self.dvc.dvc_dir).all()
        self.assertEquals(len(flist), 2)
        self.assertTrue(self.cache1 in flist)
        self.assertTrue(self.cache2 in flist)

    def test_get(self):
        cache = Cache(self.dvc.root_dir, self.dvc.dvc_dir).get(self.cache1_md5)
        self.assertEquals(cache, self.cache1)


class TestCacheLoadBadDirCache(TestDvc):
    def _do_test(self, ret):
        self.assertTrue(isinstance(ret, list))
        self.assertEqual(len(ret), 0)

    def test(self):
        fname = 'not-json'
        self.create(fname, '<clearly>not,json')
        self._do_test(Cache.load_dir_cache(fname))

        fname = 'not-list'
        self.create(fname, '{"a": "b"}')
        self._do_test(Cache.load_dir_cache(fname))


class TestExternalCacheDir(TestDvc):
    def test(self):
        cache_dir = tempfile.mkdtemp()

        ret = main(['config', 'cache.dir', cache_dir])
        self.assertEqual(ret, 0)

        shutil.rmtree('.dvc/cache')

        main(['add', self.FOO])
        self.assertEqual(ret, 0)

        main(['add', self.DATA_DIR])
        self.assertEqual(ret, 0)

        self.assertFalse(os.path.exists('.dvc/cache'))
        self.assertNotEquals(len(os.listdir(cache_dir)), 0)
