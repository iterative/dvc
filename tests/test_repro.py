import os

from tests.basic_env import TestDvc


class ReproBasicEnv(TestDvc):
    def setUp(self):
        super(ReproBasicEnv, self).setUp()

        os.mkdir('data')

        self.file_name1 = os.path.join('data', 'file1')
        self.file1_code_file = 'file1.py'
        self.create(self.file1_code_file, 'print("Hello")' + os.linesep + 'print("Mary")')
        stage = self.dvc.run(fname='file1.dvc',
                             outs=[self.file_name1],
                             deps=[self.file1_code_file],
                             cmd='python {} --not-repro > {}'.format(self.file1_code_file,
                                                                     self.file_name1))

        self.file_name11 = os.path.join('data', 'file11')
        self.file11_code_file = 'file11.py'
        self.create(self.file11_code_file,
                    'import sys' + os.linesep + 'print(open(sys.argv[1]).readline().strip())')

        stage = self.dvc.run(fname='file11.dvc',
                             outs=[self.file_name11],
                             deps=[self.file_name1, self.file11_code_file],
                             cmd='python {} {} > {}'.format(self.file11_code_file,
                                                            self.file_name1,
                                                            self.file_name11))
        self.file_name11_stage_name = stage.path

        self.file_name2 = os.path.join('data', 'file2')
        self.file2_code_file = 'file2.py'
        self.create(self.file2_code_file, 'print("Bobby")')
        self.dvc.run(fname='file2.dvc',
                     outs=[self.file_name2],
                     cmd='python {} --not-repro > {}'.format(self.file2_code_file,
                                                             self.file_name2))

        self.file_res_code_file = 'code_res.py'
        self.create(self.file_res_code_file,
                    'import sys' + os.linesep +
                    'text1 = open(sys.argv[1]).read()' + os.linesep +
                    'text2 = open(sys.argv[2]).read()' + os.linesep +
                    'print(text1 + text2)')
        self.file_name_res = os.path.join('data', 'file_res')
        stage = self.dvc.run(fname='file_res.dvc',
                             outs=[self.file_name_res],
                             deps=[self.file_res_code_file, self.file_name11, self.file_name2],
                             cmd='python {} {} {} > {}'.format(self.file_res_code_file,
                                                               self.file_name11,
                                                               self.file_name2,
                                                               self.file_name_res))
        self.file_name_res_stage_name = stage.path

    def modify_file(self, filename, content_to_add=' '):
        fd = open(filename, 'a')
        fd.write(content_to_add)
        fd.close()


class ReproCodeDependencyTest(ReproBasicEnv):
    def test(self):
        self.modify_file(self.file_res_code_file)

        self.dvc.reproduce([self.file_name_res_stage_name])

        self.assertEqual(open(self.file_name_res).read().strip(), 'Hello\nBobby')


class ReproChangedDependency(ReproBasicEnv):
    def test(self):
        self.recreate_file1()

        self.dvc.reproduce([self.file_name11_stage_name])

        self.assertEqual(open(self.file_name11).read(), 'Goodbye\n')

    def recreate_file1(self):
        self.dvc.remove(self.file_name1)

        file1_code_file = 'file1_2.py'
        self.create(file1_code_file, 'print("Goodbye")' + os.linesep + 'print("Jack")')
        self.dvc.run(fname='file1.dvc',
                     outs=[self.file_name1],
                     deps=[file1_code_file],
                     cmd='python {} --not-repro > {}'.format(file1_code_file, self.file_name1))


class ReproChangedDeepDependency(ReproChangedDependency):
    def test(self):
        self.recreate_file1()

        self.dvc.reproduce([self.file_name_res_stage_name])

        self.assertEqual(open(self.file_name_res).read().strip(), 'Goodbye\nBobby')
