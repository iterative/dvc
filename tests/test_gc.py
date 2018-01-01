import os
import shutil
import filecmp

from dvc.data_cloud import file_md5
from dvc.stage import Stage, OutputNoCacheError, OutputOutsideOfRepoError
from dvc.stage import OutputDoesNotExistError, OutputIsNotFileError
from dvc.project import StageNotFoundError

from tests.basic_env import TestDvc


class TestGC(TestDvc):
    def test_gc(self):
        stage = self.dvc.add(self.FOO)
        good_cache = self.dvc.cache.all()

        bad_cache = []
        for i in ['1', '2', '3']:
            path = os.path.join(self.dvc.cache.cache_dir, i)
            self.create(path, i)
            bad_cache.append(path)

        self.dvc.gc()

        self.assertTrue(os.path.isdir(self.dvc.cache.cache_dir))
        for c in bad_cache:
            self.assertFalse(os.path.exists(c))

        for c in good_cache:
            self.assertTrue(os.path.exists(c))
            self.assertTrue(os.path.isfile(c))
