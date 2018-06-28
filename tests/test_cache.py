import os
import shutil
import tempfile

from dvc.cache import Cache
from dvc.system import System
from dvc.main import main
from dvc.remote.local import RemoteLOCAL

from tests.basic_env import TestDvc, TestDir


class TestCache(TestDvc):
    def setUp(self):
        super(TestCache, self).setUp()
        self.cache1_md5 = '123'
        self.cache2_md5 = '234'
        self.cache1 = os.path.join(self.dvc.cache.local.cache_dir,
                                   self.cache1_md5[0:2],
                                   self.cache1_md5[2:])
        self.cache2 = os.path.join(self.dvc.cache.local.cache_dir,
                                   self.cache2_md5[0:2],
                                   self.cache2_md5[2:])
        self.create(self.cache1, '1')
        self.create(self.cache2, '2')

    def test_all(self):
        md5_list = Cache(self.dvc).local.all()
        self.assertEquals(len(md5_list), 2)
        self.assertTrue(self.cache1_md5 in md5_list)
        self.assertTrue(self.cache2_md5 in md5_list)

    def test_get(self):
        cache = Cache(self.dvc).local.get(self.cache1_md5)
        self.assertEquals(cache, self.cache1)


class TestCacheLoadBadDirCache(TestDvc):
    def _do_test(self, ret):
        self.assertTrue(isinstance(ret, list))
        self.assertEqual(len(ret), 0)

    def test(self):
        fname = 'not-json'
        self.create(fname, '<clearly>not,json')
        self._do_test(RemoteLOCAL.load_dir_cache(fname))

        fname = 'not-list'
        self.create(fname, '{"a": "b"}')
        self._do_test(RemoteLOCAL.load_dir_cache(fname))


class TestExternalCacheDir(TestDvc):
    def test(self):
        cache_dir = tempfile.mkdtemp()

        ret = main(['config', 'cache.dir', cache_dir])
        self.assertEqual(ret, 0)

        shutil.rmtree('.dvc/cache')

        ret = main(['add', self.FOO])
        self.assertEqual(ret, 0)

        ret = main(['add', self.DATA_DIR])
        self.assertEqual(ret, 0)

        self.assertFalse(os.path.exists('.dvc/cache'))
        self.assertNotEquals(len(os.listdir(cache_dir)), 0)


class TestSharedCacheDir(TestDir):
    def test(self):
        cache_dir = os.path.abspath(os.path.join(os.curdir, 'cache'))
        for d in ['dir1', 'dir2']:
            os.mkdir(d)
            os.chdir(d)

            ret = main(['init', '--no-scm'])
            self.assertEqual(ret, 0)

            ret = main(['config', 'cache.dir', cache_dir])
            self.assertEqual(ret, 0)

            shutil.rmtree(os.path.join('.dvc', 'cache'))

            with open('common', 'w+') as fd:
                fd.write('common')

            with open('unique', 'w+') as fd:
                fd.write(d)

            ret = main(['add', 'common', 'unique'])
            self.assertEqual(ret, 0)

            os.chdir('..')

        self.assertFalse(os.path.exists(os.path.join('dir1', '.dvc', 'cache')))
        self.assertFalse(os.path.exists(os.path.join('dir2', '.dvc', 'cache')))

        subdirs = list(filter(lambda x: os.path.isdir(os.path.join(cache_dir, x)), os.listdir(cache_dir)))
        self.assertEqual(len(subdirs), 3)
        self.assertEqual(len(os.listdir(os.path.join(cache_dir, subdirs[0]))), 1)
        self.assertEqual(len(os.listdir(os.path.join(cache_dir, subdirs[1]))), 1)
        self.assertEqual(len(os.listdir(os.path.join(cache_dir, subdirs[2]))), 1)
