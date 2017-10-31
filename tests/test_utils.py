import os
import shutil
import filecmp

from dvc import utils
from tests.basic_env import DirHierarchyEnvironment

class TestExecutor(DirHierarchyEnvironment):
    def setUp(self):
        DirHierarchyEnvironment.init_environment(self)

    def tearDown(self):
        pass

    def test_rmtree(self):
        root = 'testdir'

        os.makedirs(root + '/subdir')
        with open(root + '/file1', 'w+') as f:
            f.write('file1contents')
        with open(root + '/subdir/file2', 'w+') as f:
            f.write('file2contents')

        utils.rmtree(root)
        self.assertFalse(os.path.exists(root))

    def test_copyfile(self):
        src = 'file1'
        dest = 'file2'
        dest_dir = 'testdir'

        with open(src, 'w+') as f:
            f.write('file1contents')

        os.mkdir(dest_dir)

        utils.copyfile(src, dest)
        self.assertTrue(filecmp.cmp(src, dest))

        utils.copyfile(src, dest_dir)
        self.assertTrue(filecmp.cmp(src, '{}/{}'.format(dest_dir, src)))

        shutil.rmtree(dest_dir)
        os.remove(src)
        os.remove(dest)

    def test_map_progress(self):
        def f(target):
            with open(target, 'w+') as o:
                o.write(target)

        targets = ['map{}'.format(i) for i in range(1, 10)]
        n_threads = [1, 10, 20]

        for n in n_threads:
            utils.map_progress(f, targets, n)
