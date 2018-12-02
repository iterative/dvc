import os
import mock
import time
import shutil
import filecmp
import subprocess

from dvc.project import Project
from dvc.main import main
from dvc.utils import file_md5
from dvc.stage import Stage, StageFileBadNameError, MissingDep
from dvc.stage import StageBadCwdError
from dvc.command.run import CmdRun
from dvc.exceptions import (OutputDuplicationError,
                            CircularDependencyError,
                            WorkingDirectoryAsOutputError)

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

    def test_outs_no_cache(self):
        with self.assertRaises(CircularDependencyError):
            self.dvc.run(cmd='',
                         deps=[self.FOO],
                         outs_no_cache=[self.FOO],
                         fname='circular-dependency.dvc')

    def test_non_normalized_paths(self):
        with self.assertRaises(CircularDependencyError):
            self.dvc.run(cmd='',
                         deps=['./foo'],
                         outs=['foo'],
                         fname='circular-dependency.dvc')


class TestRunWorkingDirectoryAsOutput(TestDvc):
    def test(self):
        self.dvc.run(cmd='',
                     deps=[],
                     outs=[self.DATA_DIR])

        with self.assertRaises(WorkingDirectoryAsOutputError):
            self.dvc.run(cmd='',
                         cwd=self.DATA_DIR,
                         outs=[self.FOO],
                         fname='inside-cwd.dvc')


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

    def test_run_args_with_spaces(self):
        with open(self.CODE, 'w') as fobj:
            fobj.write("import sys\nopen(sys.argv[1], 'w+').write(sys.argv[2])")

        arg = 'arg1 arg2'
        log = 'log'
        ret = main(['run',
                    'python',
                    self.CODE,
                    log,
                    'arg1 arg2'])

        self.assertEqual(ret, 0)

        with open(log, 'r') as fobj:
            self.assertEqual(fobj.read(), arg)

    @mock.patch.object(subprocess, 'Popen', side_effect=KeyboardInterrupt)
    def test_keyboard_interrupt(self, mock_popen):
        ret = main(['run', 'mycmd'])
        self.assertEqual(ret, 252)

    @mock.patch.object(Project, 'run')
    def test_deterministic(self, mock_run):
        ret = main(['run',
                    '-d', self.FOO,
                    '-d', self.CODE,
                    '--deterministic',
                    '-o', 'out',
                    '-f', 'out.dvc',
                    'python', self.CODE, self.FOO, 'out'])
        self.assertEqual(ret, 0)
        mock_run.assert_called_once()
        args, kwargs = mock_run.call_args
        self.assertIn(('deterministic', True), kwargs.items())


class TestRunDeterministic(TestDvc):
    def test(self):
        out_file = 'out'
        stage_file = out_file + '.dvc'
        cmd = 'python {} {} {}'.format(self.CODE, self.FOO, out_file)
        self.dvc.add(self.FOO)
        stage = self.dvc.run(cmd=cmd,
                             fname=stage_file,
                             deterministic=False,
                             overwrite=True,
                             deps=[self.FOO, self.CODE],
                             outs=[out_file])
        self.assertTrue(stage is not None)

        # save timestamps to make sure that files didn't change.
        stage_file_mtime_1 = os.path.getmtime(stage_file)
        out_file_mtime_1 = os.path.getmtime(out_file)

        # sleep to make sure that mtime changes even on filesystems
        # with very low timestamp resolution(e.g. 1 sec on APFS).
        time.sleep(2)

        # dependencies didn't change, so deterministic stage shouldn't
        # be ran again.
        stage = self.dvc.run(cmd=cmd,
                             fname=stage_file,
                             deterministic=True,
                             overwrite=True,
                             deps=[self.FOO, self.CODE],
                             outs=[out_file])
        self.assertTrue(stage is None)

        stage_file_mtime_2 = os.path.getmtime(stage_file)
        out_file_mtime_2 = os.path.getmtime(out_file)

        self.assertEqual(stage_file_mtime_1, stage_file_mtime_2)
        self.assertEqual(out_file_mtime_1, out_file_mtime_2)

        time.sleep(2)

        # now change one of the deps and make sure stage did run
        os.unlink(self.FOO)
        shutil.copy(self.BAR, self.FOO)

        stage = self.dvc.run(cmd=cmd,
                             fname=stage_file,
                             deterministic=True,
                             overwrite=True,
                             deps=[self.FOO, self.CODE],
                             outs=[out_file])
        self.assertTrue(stage is not None)

        stage_file_mtime_3 = os.path.getmtime(stage_file)
        out_file_mtime_3 = os.path.getmtime(out_file)

        self.assertNotEqual(stage_file_mtime_2, stage_file_mtime_3)
        self.assertNotEqual(out_file_mtime_2, out_file_mtime_3)
