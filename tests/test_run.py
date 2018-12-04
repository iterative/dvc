import os
import mock
import time
import shutil
import filecmp
import subprocess

from dvc.project import Project
from dvc.main import main
from dvc.utils import file_md5
from dvc.system import System
from dvc.stage import Stage, StageFileBadNameError, MissingDep
from dvc.stage import StageBadCwdError, StageFileAlreadyExistsError
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


class TestRunRemoveOuts(TestDvc):
    def test(self):
        with open(self.CODE, 'w+') as fobj:
            fobj.write("import sys\n")
            fobj.write("import os\n")
            fobj.write("if os.path.exists(sys.argv[1]):\n")
            fobj.write("    sys.exit(1)\n")
            fobj.write("open(sys.argv[1], 'w+').close()\n")

        ret = main(['run',
                    '--remove-outs',
                    '-d', self.CODE,
                    '-o', self.FOO,
                    'python', self.CODE, self.FOO])
        self.assertEqual(ret, 0)


class TestRunUnprotectOutsCopy(TestDvc):
    def test(self):
        with open(self.CODE, 'w+') as fobj:
            fobj.write("import sys\n")
            fobj.write("with open(sys.argv[1], 'a+') as fobj:\n")
            fobj.write("    fobj.write('foo')\n")

        ret = main(['config', 'cache.protected', 'true'])
        self.assertEqual(ret, 0)

        ret = main(['config', 'cache.type', 'copy'])
        self.assertEqual(ret, 0)

        ret = main(['run',
                    '-d', self.CODE,
                    '-o', self.FOO,
                    'python', self.CODE, self.FOO])
        self.assertEqual(ret, 0)
        self.assertFalse(os.access(self.FOO, os.W_OK))
        self.assertEqual(open(self.FOO, 'r').read(), 'foofoo')

        ret = main(['run',
                    '--overwrite-dvcfile',
                    '--ignore-build-cache',
                    '-d', self.CODE,
                    '-o', self.FOO,
                    'python', self.CODE, self.FOO])
        self.assertEqual(ret, 0)
        self.assertFalse(os.access(self.FOO, os.W_OK))
        self.assertEqual(open(self.FOO, 'r').read(), 'foofoofoo')


class TestRunUnprotectOutsSymlink(TestDvc):
    def test(self):
        with open(self.CODE, 'w+') as fobj:
            fobj.write("import sys\n")
            fobj.write("import os\n")
            fobj.write("assert os.path.exists(sys.argv[1])\n")
            fobj.write("with open(sys.argv[1], 'a+') as fobj:\n")
            fobj.write("    fobj.write('foo')\n")

        ret = main(['config', 'cache.protected', 'true'])
        self.assertEqual(ret, 0)

        ret = main(['config', 'cache.type', 'symlink'])
        self.assertEqual(ret, 0)

        self.assertEqual(ret, 0)
        ret = main(['run',
                    '-d', self.CODE,
                    '-o', self.FOO,
                    'python', self.CODE, self.FOO])
        self.assertEqual(ret, 0)
        self.assertFalse(os.access(self.FOO, os.W_OK))
        self.assertTrue(System.is_symlink(self.FOO))
        self.assertEqual(open(self.FOO, 'r').read(), 'foofoo')

        ret = main(['run',
                    '--overwrite-dvcfile',
                    '--ignore-build-cache',
                    '-d', self.CODE,
                    '-o', self.FOO,
                    'python', self.CODE, self.FOO])
        self.assertEqual(ret, 0)
        self.assertFalse(os.access(self.FOO, os.W_OK))
        self.assertTrue(System.is_symlink(self.FOO))
        self.assertEqual(open(self.FOO, 'r').read(), 'foofoofoo')


class TestRunUnprotectOutsHardlink(TestDvc):
    def test(self):
        with open(self.CODE, 'w+') as fobj:
            fobj.write("import sys\n")
            fobj.write("import os\n")
            fobj.write("assert os.path.exists(sys.argv[1])\n")
            fobj.write("with open(sys.argv[1], 'a+') as fobj:\n")
            fobj.write("    fobj.write('foo')\n")

        ret = main(['config', 'cache.protected', 'true'])
        self.assertEqual(ret, 0)

        ret = main(['config', 'cache.type', 'hardlink'])
        self.assertEqual(ret, 0)

        self.assertEqual(ret, 0)
        ret = main(['run',
                    '-d', self.CODE,
                    '-o', self.FOO,
                    'python', self.CODE, self.FOO])
        self.assertEqual(ret, 0)
        self.assertFalse(os.access(self.FOO, os.W_OK))
        self.assertTrue(System.is_hardlink(self.FOO))
        self.assertEqual(open(self.FOO, 'r').read(), 'foofoo')

        ret = main(['run',
                    '--overwrite-dvcfile',
                    '--ignore-build-cache',
                    '-d', self.CODE,
                    '-o', self.FOO,
                    'python', self.CODE, self.FOO])
        self.assertEqual(ret, 0)
        self.assertFalse(os.access(self.FOO, os.W_OK))
        self.assertTrue(System.is_hardlink(self.FOO))
        self.assertEqual(open(self.FOO, 'r').read(), 'foofoofoo')


class TestCmdRunOverwrite(TestDvc):
    def test(self):
        ret = main(['run',
                    '-d', self.FOO,
                    '-d', self.CODE,
                    '-o', 'out',
                    '-f', 'out.dvc',
                    'python', self.CODE, self.FOO, 'out'])
        self.assertEqual(ret, 0)

        stage_mtime = os.path.getmtime('out.dvc')

        ret = main(['run',
                    '-d', self.FOO,
                    '-d', self.CODE,
                    '-o', 'out',
                    '-f', 'out.dvc',
                    'python', self.CODE, self.FOO, 'out'])
        self.assertEqual(ret, 0)

        # NOTE: check that dvcfile was NOT overwritten
        self.assertEqual(stage_mtime, os.path.getmtime('out.dvc'))
        stage_mtime = os.path.getmtime('out.dvc')

        ret = main(['run',
                    '-d', self.FOO,
                    '-d', self.CODE,
                    '--overwrite-dvcfile',
                    '--ignore-build-cache',
                    '-o', 'out',
                    '-f', 'out.dvc',
                    'python', self.CODE, self.FOO, 'out'])
        self.assertEqual(ret, 0)

        # NOTE: check that dvcfile was overwritten
        self.assertNotEqual(stage_mtime, os.path.getmtime('out.dvc'))
        stage_mtime = os.path.getmtime('out.dvc')

        ret = main(['run',
                    '--overwrite-dvcfile',
                    '-f', 'out.dvc',
                    '-d', self.BAR])
        self.assertEqual(ret, 0)

        # NOTE: check that dvcfile was overwritten
        self.assertNotEqual(stage_mtime, os.path.getmtime('out.dvc'))


class TestRunDeterministicBase(TestDvc):
    def setUp(self):
        super(TestRunDeterministicBase, self).setUp()
        self.out_file = 'out'
        self.stage_file = self.out_file + '.dvc'
        self.cmd = 'python {} {} {}'.format(self.CODE, self.FOO, self.out_file)
        self.deps = [self.FOO, self.CODE]
        self.outs = [self.out_file]
        self.overwrite = False
        self.ignore_build_cache = False

        self._run()

    def _run(self):
        self.stage = self.dvc.run(cmd=self.cmd,
                                  fname=self.stage_file,
                                  overwrite=self.overwrite,
                                  ignore_build_cache=self.ignore_build_cache,
                                  deps=self.deps,
                                  outs=self.outs)


class TestRunDeterministic(TestRunDeterministicBase):
    def test(self):
        self._run()


class TestRunDeterministicOverwrite(TestRunDeterministicBase):
    def test(self):
        self.overwrite = True
        self.ignore_build_cache = True
        self._run()


class TestRunDeterministicCallback(TestRunDeterministicBase):
    def test(self):
        self.stage.remove()
        self.deps = []
        self._run()
        self._run()


class TestRunDeterministicChangedDep(TestRunDeterministicBase):
    def test(self):
        os.unlink(self.FOO)
        shutil.copy(self.BAR, self.FOO)
        with self.assertRaises(StageFileAlreadyExistsError):
           self._run()


class TestRunDeterministicChangedDepsList(TestRunDeterministicBase):
    def test(self):
        self.deps = [self.BAR, self.CODE]
        with self.assertRaises(StageFileAlreadyExistsError):
           self._run()


class TestRunDeterministicNewDep(TestRunDeterministicBase):
    def test(self):
        self.deps = [self.FOO, self.BAR, self.CODE]
        with self.assertRaises(StageFileAlreadyExistsError):
           self._run()


class TestRunDeterministicRemoveDep(TestRunDeterministicBase):
    def test(self):
        self.deps = [self.CODE]
        with self.assertRaises(StageFileAlreadyExistsError):
           self._run()


class TestRunDeterministicChangedOut(TestRunDeterministicBase):
    def test(self):
        os.unlink(self.out_file)
        self.out_file_mtime = None
        with self.assertRaises(StageFileAlreadyExistsError):
           self._run()


class TestRunDeterministicChangedCmd(TestRunDeterministicBase):
    def test(self):
        self.cmd += ' arg'
        with self.assertRaises(StageFileAlreadyExistsError):
           self._run()
