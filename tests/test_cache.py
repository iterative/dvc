import os

from dvc.cache import Cache
from dvc.system import System

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
        flist = Cache(self.dvc.dvc_dir).all()
        self.assertEquals(len(flist), 2)
        self.assertTrue(self.cache1 in flist)
        self.assertTrue(self.cache2 in flist)

    def test_get(self):
        cache = Cache(self.dvc.dvc_dir).get(self.cache1_md5)
        self.assertEquals(cache, self.cache1)

    def test_find_cache(self):
        fname1 = os.path.basename(self.cache1)
        fname1_md5 = self.cache1_md5
        fname2 = os.path.basename(self.cache2)
        fname2_md5 = self.cache2_md5
        fname3 = 'non_existing'

        System.hardlink(self.cache1, fname1)
        System.hardlink(self.cache2, fname2)

        cache = Cache(self.dvc.dvc_dir).find_cache([fname1, fname2, fname3])

        expected = {fname1: fname1_md5, fname2: fname2_md5}

        self.assertEqual(len(cache), 2)
        self.assertEqual(cache, expected)
