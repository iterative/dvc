import os
import stat
import shutil
import filecmp

from dvc.main import main
from dvc.command.repro import CmdRepro
from dvc.project import ReproductionError
from dvc.utils import file_md5

from tests.basic_env import TestDvc


class TestRepro(TestDvc):
    def setUp(self):
        super(TestRepro, self).setUp()

        self.foo_stage = self.dvc.add(self.FOO)

        self.file1 = 'file1'
        self.file1_stage = self.file1 + '.dvc'
        self.dvc.run(fname=self.file1_stage,
                     outs=[self.file1],
                     deps=[self.FOO, self.CODE],
                     cmd='python {} {} {}'.format(self.CODE, self.FOO, self.file1))


class TestReproChangedCode(TestRepro):
    def test(self):
        self.swap_code()

        stages = self.dvc.reproduce(self.file1_stage)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR))
        self.assertEqual(len(stages), 1)

    def swap_code(self):
        os.unlink(self.CODE)
        new_contents = self.CODE_CONTENTS
        new_contents += "\nshutil.copyfile('{}', sys.argv[2])\n".format(self.BAR)
        self.create(self.CODE, new_contents)


class TestReproChangedData(TestRepro):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.file1_stage)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR))
        self.assertEqual(len(stages), 2)

    def swap_foo_with_bar(self):
        os.chmod(self.FOO, stat.S_IWRITE)
        os.unlink(self.FOO)
        shutil.copyfile(self.BAR, self.FOO)


class TestReproChangedDeepData(TestReproChangedData):
    def test(self):
        file2 = 'file2'
        file2_stage = file2 + '.dvc'
        self.dvc.run(fname=file2_stage,
                     outs=[file2],
                     deps=[self.file1, self.CODE],
                     cmd='python {} {} {}'.format(self.CODE, self.file1, file2))

        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(file2_stage)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR))
        self.assertTrue(filecmp.cmp(file2, self.BAR))
        self.assertEqual(len(stages), 3)


class TestReproPhony(TestReproChangedData):
    def test(self):
        stage = self.dvc.run(deps=[self.file1])

        self.swap_foo_with_bar()

        self.dvc.reproduce(stage.path)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR))


class TestNonExistingOutput(TestRepro):
    def test(self):
        os.chmod(self.FOO, stat.S_IWRITE)
        os.unlink(self.FOO)

        with self.assertRaises(ReproductionError) as cx:
            self.dvc.reproduce(self.file1_stage)


class TestReproDataSource(TestReproChangedData):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.foo_stage.path)

        self.assertTrue(filecmp.cmp(self.FOO, self.BAR))
        self.assertEqual(stages[0].outs[0].md5, file_md5(self.BAR)[0])


class TestReproChangedDir(TestDvc):
    def test(self):
        file_name = 'file'
        shutil.copyfile(self.FOO, file_name)

        stage_name = 'dir.dvc'
        dir_name = 'dir'
        dir_code = 'dir.py'

        with open(dir_code, 'w+') as fd:
            fd.write("import os; import shutil; os.mkdir(\"{}\"); shutil.copyfile(\"{}\", os.path.join(\"{}\", \"{}\"))".format(dir_name, file_name, dir_name, file_name))

        self.dvc.run(fname=stage_name,
                     outs=[dir_name],
                     deps=[file_name, dir_code],
                     cmd="python {}".format(dir_code))

        stages = self.dvc.reproduce(stage_name)
        self.assertEqual(len(stages), 0)

        os.unlink(file_name)
        shutil.copyfile(self.BAR, file_name)

        stages = self.dvc.reproduce(stage_name)
        self.assertEqual(len(stages), 1)


class TestCmdRepro(TestRepro):
    def test(self):
        ret = main(['repro',
                    self.file1_stage])
        self.assertEqual(ret, 0)

        ret = main(['repro',
                    'non-existing-file'])
        self.assertNotEqual(ret, 0)
