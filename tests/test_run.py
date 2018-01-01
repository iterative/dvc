import os
import filecmp

from dvc.data_cloud import file_md5

from tests.basic_env import TestDvc


class TestRun(TestDvc):
    def test(self):
        cmd = 'python {} {} {}'.format(self.CODE, self.FOO, 'out')
        deps = [self.FOO]
        deps_no_cache = [self.CODE]
        outs = [os.path.join(self.dvc.root_dir, 'out')]
        outs_no_cache = []
        locked = False
        fname = os.path.join(self.dvc.root_dir, 'out.dvc')
        cwd = os.curdir

        stage = self.dvc.run(cmd, deps, deps_no_cache, outs, outs_no_cache, locked, fname, cwd)

        self.assertTrue(filecmp.cmp(self.FOO, 'out'))
        self.assertTrue(os.path.isfile(stage.path))
        self.assertEqual(stage.cmd, cmd)
        self.assertEqual(len(stage.deps), len(deps + deps_no_cache))
        self.assertEqual(len(stage.outs), len(outs + outs_no_cache))
        self.assertEqual(stage.outs[0].path, outs[0])
        self.assertEqual(stage.outs[0].md5, file_md5(self.FOO)[0])  
        self.assertEqual(stage.locked, locked)
        self.assertTrue(stage.path, fname)


class TestRunEmpty(TestDvc):
    def test(self):
        self.dvc.run('', [], [], [], [], False, 'empty.dvc', os.curdir)
