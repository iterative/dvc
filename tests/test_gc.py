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
        self.good_cache = self.dvc.cache.local.all()

        self.bad_cache = []
        for i in ['123', '234', '345']:
            path = os.path.join(self.dvc.cache.local.cache_dir, i[0:2], i[2:])
            self.create(path, i)
            self.bad_cache.append(path)

    def test_api(self):
        self.dvc.gc()
        self._test_gc()

    def test_cli(self):
        ret = main(['gc'])
        self._test_gc()

    def _test_gc(self):
        self.assertTrue(os.path.isdir(self.dvc.cache.local.cache_dir))
        for c in self.bad_cache:
            self.assertFalse(os.path.exists(c))

        for c in self.good_cache:
            self.assertTrue(os.path.exists(c))
