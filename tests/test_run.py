import os
import filecmp

from dvc.main import main
from dvc.utils import file_md5
from dvc.stage import Stage, StageFileBadNameError, MissingDep
from dvc.stage import StageBadCwdError
from dvc.command.run import CmdRun
from dvc.exceptions import OutputDuplicationError, CircularDependencyError

from tests.basic_env import TestDvc


class TestRun(TestDvc):
    def test(self):
        cmd = 'python {} {} {}'.format(self.CODE, self.FOO, 'out')
        deps = [self.FOO, self.CODE]
        outs = [os.path.join(self.dvc.root_dir, 'out')]
        outs_no_cache = []
        fname = 'out.dvc'
        cwd = os.curdir

        self.dvc.add(self.FOO)
        stage = self.dvc.run(cmd=cmd,
                             deps=deps,
                             outs=outs,
                             outs_no_cache=outs_no_cache,
                             fname=fname,
                             cwd=cwd)

        self.assertTrue(filecmp.cmp(self.FOO, 'out', shallow=False))
        self.assertTrue(os.path.isfile(stage.path))
        self.assertEqual(stage.cmd, cmd)
        self.assertEqual(len(stage.deps), len(deps))
        self.assertEqual(len(stage.outs), len(outs + outs_no_cache))
        self.assertEqual(stage.outs[0].path, outs[0])
        self.assertEqual(stage.outs[0].md5, file_md5(self.FOO)[0])  
        self.assertTrue(stage.path, fname)

        with self.assertRaises(OutputDuplicationError):
            stage = self.dvc.run(cmd=cmd,
                                 deps=deps,
                                 outs=outs,
                                 outs_no_cache=outs_no_cache,
                                 fname='duplicate' + fname,
                                 cwd=cwd)


class TestRunEmpty(TestDvc):
    def test(self):
        self.dvc.run(cmd='',
                     deps=[],
                     outs=[],
                     outs_no_cache=[],
                     fname='empty.dvc',
                     cwd=os.curdir)


class TestRunMissingDep(TestDvc):
    def test(self):
        with self.assertRaises(MissingDep):
            self.dvc.run(cmd='',
                         deps=['non-existing-dep'],
                         outs=[],
                         outs_no_cache=[],
                         fname='empty.dvc',
                         cwd=os.curdir)


class TestRunBadStageFilename(TestDvc):
    def test(self):
        with self.assertRaises(StageFileBadNameError):
            self.dvc.run(cmd='',
                         deps=[],
                         outs=[],
                         outs_no_cache=[],
                         fname='empty',
                         cwd=os.curdir)

        with self.assertRaises(StageFileBadNameError):
            self.dvc.run(cmd='',
                         deps=[],
                         outs=[],
                         outs_no_cache=[],
                         fname=os.path.join(self.DATA_DIR, 'empty.dvc'),
                         cwd=os.curdir)



class TestRunNoExec(TestDvc):
    def test(self):
        self.dvc.run(cmd='python {} {} {}'.format(self.CODE, self.FOO, 'out'),
                     no_exec=True)
        self.assertFalse(os.path.exists('out'))


class TestRunCircularDependency(TestDvc):
    def test(self):
        with self.assertRaises(CircularDependencyError):
            self.dvc.run(cmd='',
                         deps=[self.FOO],
                         outs=[self.FOO],
                         fname='circular-dependency.dvc')

class TestRunBadCwd(TestDvc):
    def test(self):
        with self.assertRaises(StageBadCwdError):
            self.dvc.run(cmd='',
                         cwd=self.mkdtemp())


class TestCmdRun(TestDvc):
    def test_run(self):
        ret = main(['run',
                    '-d', self.FOO,
                    '-d', self.CODE,
                    '-o', 'out',
                    '-f', 'out.dvc',
                    'python', self.CODE, self.FOO, 'out'])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile('out'))
        self.assertTrue(os.path.isfile('out.dvc'))
        self.assertTrue(filecmp.cmp(self.FOO, 'out', shallow=False))

    def test_run_bad_command(self):
        ret = main(['run',
                    'non-existing-command'])
        self.assertNotEqual(ret, 0)
