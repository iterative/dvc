import os
import shutil
import filecmp

from dvc.main import main
from dvc.utils import file_md5
from dvc.stage import Stage
from dvc.command.gc import CmdGC

from tests.basic_env import TestDvc


class TestGC(TestDvc):
    def setUp(self):
        super(TestGC, self).setUp()

        self.dvc.add(self.FOO)
        self.dvc.add(self.DATA_DIR)
        self.good_cache = [self.dvc.cache.local.get(md5) for md5 in self.dvc.cache.local.all()]

        self.bad_cache = []
        for i in ['123', '234', '345']:
            path = os.path.join(self.dvc.cache.local.cache_dir, i[0:2], i[2:])
            self.create(path, i)
            self.bad_cache.append(path)

    def test_api(self):
        self.dvc.gc()
        self._test_gc()

    def test_cli(self):
        ret = main(['gc', '-f'])
        self.assertEqual(ret, 0)
        self._test_gc()

    def _test_gc(self):
        self.assertTrue(os.path.isdir(self.dvc.cache.local.cache_dir))
        for c in self.bad_cache:
            self.assertFalse(os.path.exists(c))

        for c in self.good_cache:
            self.assertTrue(os.path.exists(c))


class TestGCBranchesTags(TestDvc):
    def _check_cache(self, num):
        total = 0
        for root, dirs, files in os.walk(os.path.join('.dvc', 'cache')):
            total += len(files)
        self.assertEqual(total, num)

    def test(self):
        fname = 'file'

        with open(fname, 'w+') as fobj:
            fobj.write('v1.0')

        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)
        self.dvc.scm.add(['.gitignore', stages[0].relpath])
        self.dvc.scm.commit('v1.0')
        self.dvc.scm.tag('v1.0')

        self.dvc.scm.checkout('test', create_new=True)
        self.dvc.remove(stages[0].relpath, outs_only=True)
        with open(fname, 'w+') as fobj:
            fobj.write('test')
        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)
        self.dvc.scm.add(['.gitignore', stages[0].relpath])
        self.dvc.scm.commit('test')

        self.dvc.scm.checkout('master')
        self.dvc.remove(stages[0].relpath, outs_only=True)
        with open(fname, 'w+') as fobj:
            fobj.write('trash')
        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)
        self.dvc.scm.add(['.gitignore', stages[0].relpath])
        self.dvc.scm.commit('trash')

        self.dvc.remove(stages[0].relpath, outs_only=True)
        with open(fname, 'w+') as fobj:
            fobj.write('master')
        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)
        self.dvc.scm.add(['.gitignore', stages[0].relpath])
        self.dvc.scm.commit('master')

        self._check_cache(4)

        self.dvc.gc(all_tags=True, all_branches=True)

        self._check_cache(3)

        self.dvc.gc(all_tags=False, all_branches=True)

        self._check_cache(2)

        self.dvc.gc(all_tags=True, all_branches=False)

        self._check_cache(1)
