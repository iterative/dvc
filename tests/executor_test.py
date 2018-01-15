import os
import shutil
import tempfile
from unittest import TestCase

from dvc.executor import Executor, ExecutorError
from dvc.system import System


class TestExecutor(TestCase):
    def setUp(self):
        self.test_dir = System.get_long_path(tempfile.mkdtemp())

        self.code_file = 'myfile.py'
        fd = open(self.code_file, 'w')
        fd.write('import sys\nsys.stdout.write("111")\nsys.stderr.write("222")\n')
        fd.close()

    def tearDown(self):
        shutil.rmtree(self.test_dir)
        if os.path.exists('myfile.py'):
            os.remove('myfile.py')

    def test_suppress_output(self):
        output = Executor.exec_cmd_only_success(['python', self.code_file])
        self.assertEqual(output, '111')

    def test_output_to_std_console(self):
        # This output "111" in stdout and "222" are not suppressible.
        # You will see the outputs when the unit-test runs.
        output = Executor.exec_cmd_only_success(['python', self.code_file], '-', '-')
        self.assertEqual(output, '')

    def test_custom_file_outputs(self):
        stdout_file = os.path.join(self.test_dir, 'stdout.txt')
        stderr_file = os.path.join(self.test_dir, 'stderr.txt')

        output = Executor.exec_cmd_only_success(['python', self.code_file], stdout_file, stderr_file)

        self.assertEqual(output, '')
        self.assertEqual(open(stdout_file).read(), "111")
        self.assertEqual(open(stderr_file).read(), "222")

        os.remove(stdout_file)
        os.remove(stderr_file)

    def test_wrong_command(self):
        with self.assertRaises(ExecutorError):
            Executor.exec_cmd_only_success(['--wrong-command--', self.test_dir])
