import os

from dvc.command.remove import CmdRemove
from dvc.command.repro import CmdRepro
from dvc.command.run import CmdRun
from dvc.executor import Executor
from tests.test_cmd_run import RunBasicTest


class ReproBasicEnv(RunBasicTest):
    def setUp(self):
        super(ReproBasicEnv, self).setUp()

        self.file_name1 = os.path.join('data', 'file1')
        self.file1_code_file = 'file1.py'
        self.create_file_and_commit(self.file1_code_file, 'print("Hello")' + os.linesep + 'print("Mary")')
        self.settings._args = ['python', self.file1_code_file, '--not-repro',
                                '--stdout', self.file_name1, '--code', self.file1_code_file]
        cmd_file1 = CmdRun(self.settings)
        self.assertEqual(cmd_file1.code_dependencies, [self.file1_code_file])
        cmd_file1.run()

        self.file_name11 = os.path.join('data', 'file11')
        self.file11_code_file = 'file11.py'
        self.create_file_and_commit(self.file11_code_file,
                                    'import sys' + os.linesep + 'print(open(sys.argv[1]).readline().strip())')

        self.settings._args = ['python', self.file11_code_file, self.file_name1,
                               '--stdout', self.file_name11, '--code', self.file11_code_file]
        CmdRun(self.settings).run()

        self.file_name2 = os.path.join('data', 'file2')
        self.file2_code_file = 'file2.py'
        self.create_file_and_commit(self.file2_code_file,
                                    'print("Bobby")')
        self.settings._args = ['python', self.file2_code_file,
                               '--not-repro', '--stdout', self.file_name2]
        CmdRun(self.settings).run()

        self.file_res_code_file = 'code_res.py'
        self.create_file_and_commit(self.file_res_code_file,
                                    'import sys' + os.linesep +
                                    'text1 = open(sys.argv[1]).read()' + os.linesep +
                                    'text2 = open(sys.argv[2]).read()' + os.linesep +
                                    'print(text1 + text2)')
        self.file_name_res = os.path.join('data', 'file_res')
        self.settings._args = ['python', self.file_res_code_file,
                               self.file_name11,
                               self.file_name2,
                               '--stdout', self.file_name_res,
                               '--code', self.file_res_code_file]
        cmd_res = CmdRun(self.settings)
        self.assertEqual(cmd_res.code_dependencies, [self.file_res_code_file])
        cmd_res.run()

        lines = list(filter(None, map(str.strip, open(self.file_name_res).readlines())))
        self.assertEqual(lines, ['Hello', 'Bobby'])

    def create_file_and_commit(self, file_name, content='Any', message='Just a commit'):
        self.create_file(file_name, content)
        self.commit_file(file_name, message)

    @staticmethod
    def commit_file(file_name, message='Just a commit'):
        Executor.exec_cmd_only_success(['git', 'add', file_name])
        Executor.exec_cmd_only_success(['git', 'commit', '-m', message])

    def modify_file_and_commit(self, filename, content_to_add=' '):
        fd = open(filename, 'a')
        fd.write(content_to_add)
        fd.close()
        self.commit_file(filename)


class ReproCodeDependencyTest(ReproBasicEnv):
    def test(self):
        self.modify_file_and_commit(self.file_res_code_file)

        self.settings._args = [self.file_name_res]
        CmdRepro(self.settings).run()

        self.assertEqual(open(self.file_name_res).read().strip(), 'Hello\nBobby')


class ReproChangedDependency(ReproBasicEnv):
    def test(self):
        self.recreate_file1()

        self.settings._args = [self.file_name11]
        CmdRepro(self.settings).run()

        self.assertEqual(open(self.file_name11).read(), 'Goodbye\n')

    def recreate_file1(self):
        self.settings._args = [self.file_name1, '--keep-in-cloud']
        CmdRemove(self.settings).run()

        file1_code_file = 'file1_2.py'
        self.create_file_and_commit(file1_code_file, 'print("Goodbye")' + os.linesep + 'print("Jack")')
        self.settings._args = ['python', file1_code_file, '--not-repro',
                               '--stdout', self.file_name1, '--code', file1_code_file]

        CmdRun(self.settings).run()


class ReproChangedDeepDependency(ReproChangedDependency):
    def test(self):
        self.recreate_file1()

        self.settings._args = [self.file_name_res]
        CmdRepro(self.settings).run()

        self.assertEqual(open(self.file_name_res).read().strip(), 'Goodbye\nBobby')
