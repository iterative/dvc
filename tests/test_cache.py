import os

from dvc.cache import Cache

from tests.basic_env import TestDvc


class TestCache(TestDvc):
    def setUp(self):
        super(TestCache, self).setUp()
        self.cache1 = os.path.join(self.dvc.cache.cache_dir, '1')
        self.cache2 = os.path.join(self.dvc.cache.cache_dir, '2')
        self.create(self.cache1, '1')
        self.create(self.cache2, '2')

    def test_all(self):
        flist = Cache(self.dvc.dvc_dir).all()
        self.assertEquals(len(flist), 2)
        self.assertTrue(self.cache1 in flist)
        self.assertTrue(self.cache2 in flist)

    def test_get(self):
        cache = Cache(self.dvc.dvc_dir).get(os.path.basename(self.cache1))
        self.assertEquals(cache, self.cache1)
