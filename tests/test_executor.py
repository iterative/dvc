import os
import shutil
import tempfile
from unittest import TestCase

from dvc.executor import Executor, ExecutorError
from dvc.utils import rmfile, rmtree


class TestExecutor(TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

        self.file1 = 'file1.txt'
        fd1 = open(os.path.join(self.test_dir, self.file1), 'w+')
        fd1.write('this is file 1')
        fd1.close()

        self.file2 = 'f.csv'
        fd2 = open(os.path.join(self.test_dir, self.file2), 'w+')
        fd2.write('this is file 222')
        fd2.close()

    def tearDown(self):
        rmtree(self.test_dir)

    def test_suppress_output(self):
        output = Executor.exec_cmd_only_success(['ls', self.test_dir])
        output_set = set(output.split())
        self.assertEqual(output_set, {self.file1, self.file2})

    def test_output_to_std_console(self):
        output = Executor.exec_cmd_only_success(['ls', self.test_dir], '-', '-')
        self.assertEqual(output, '')

    def test_custom_file_outputs(self):
        stdout_file = os.path.join(self.test_dir, 'stdout.txt')
        stderr_file = os.path.join(self.test_dir, 'stderr.txt')
        output = Executor.exec_cmd_only_success(['ls', self.test_dir], stdout_file, stderr_file)

        self.assertEqual(output, '')

        output_set = set(open(stdout_file).read().split())
        expected = {self.file1, self.file2, os.path.basename(stdout_file), os.path.basename(stderr_file)}
        self.assertEqual(output_set, expected)

        rmfile(stdout_file)
        rmfile(stderr_file)

    def test_wrong_command(self):
        with self.assertRaises(ExecutorError):
            Executor.exec_cmd_only_success(['--wrong-command--', self.test_dir])
