from dvc.command.remove import CmdDataRemove
from dvc.command.repro import ReproChange, CmdRepro
from dvc.command.run import CmdRun, RunError
from dvc.executor import Executor
from tests.test_cmd_run import RunBasicTest


class ReproBasicEnv(RunBasicTest):
    def setUp(self):
        super(ReproBasicEnv, self).setUp()

        self.file_name1 = 'data/file1'
        self.file1_code_file = 'file1.py'
        self.create_file_and_commit(self.file1_code_file, 'An awesome code...')
        cmd_file1 = CmdRun(args=[
                                'printf',
                                'Hello\nMary',
                                '--not-repro',
                                '--stdout',
                                self.file_name1,
                                '--code',
                                self.file1_code_file
                            ],
                            parse_config=False,
                            config_obj=self.config,
                            git_obj=self.git)
        self.assertEqual(cmd_file1.code_dependencies, [self.file1_code_file])
        cmd_file1.run()

        self.file_name11 = 'data/file11'
        CmdRun(args=['head', '-n', '1', self.file_name1, '--stdout', self.file_name11],
               parse_config=False,
               config_obj=self.config,
               git_obj=self.git
        ).run()

        self.file_name2 = 'data/file2'
        CmdRun(args=['printf', 'Bobby', '--not-repro', '--stdout', self.file_name2],
               parse_config=False,
               config_obj=self.config,
               git_obj=self.git
        ).run()

        self.file_res_code_file = 'code_res.py'
        self.create_file_and_commit(self.file_res_code_file, 'Another piece of code')
        self.file_name_res = 'data/file_res'
        cmd_res = CmdRun(args=[
                            'cat',
                            self.file_name11,
                            self.file_name2,
                            '--stdout',
                            self.file_name_res,
                            '--code',
                            self.file_res_code_file
                         ],
                         parse_config=False,
                         config_obj=self.config,
                         git_obj=self.git)
        self.assertEqual(cmd_res.code_dependencies, [self.file_res_code_file])
        cmd_res.run()

        self.assertEqual(open(self.file_name_res).read(), 'Hello\nBobby')

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

        CmdRepro(args=[self.file_name_res],
                 config_obj=self.config,
                 git_obj=self.git
        ).run()

        self.assertEqual(open(self.file_name_res).read(), 'Hello\nBobby')


class ReproChangedDependency(ReproBasicEnv):
    def test(self):
        self.recreate_file1()

        CmdRepro(args=[self.file_name11],
                 config_obj=self.config,
                 git_obj=self.git
        ).run()

        self.assertEqual(open(self.file_name11).read(), 'Goodbye\n')

    def recreate_file1(self):
        CmdDataRemove(args=[self.file_name1, '--keep-in-cloud'],  # ???
                      config_obj=self.config,
                      git_obj=self.git
                      ).run()
        CmdRun(args=['printf', 'Goodbye\nJack', '--stdout', self.file_name1],
               config_obj=self.config,
               git_obj=self.git
               ).run()


class ReproChangedDeepDependency(ReproChangedDependency):
    def test(self):
        self.recreate_file1()

        CmdRepro(args=[self.file_name_res],
                 config_obj=self.config,
                 git_obj=self.git
        ).run()

        self.assertEqual(open(self.file_name_res).read(), 'Goodbye\nBobby')
