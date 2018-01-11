"""
Exec wrapper.
"""
import subprocess

from dvc.exceptions import DvcException


class ExecutorError(DvcException):
    """ Executor error exception """
    def __init__(self, msg):
        DvcException.__init__(self, msg)


class Executor(object):
    """ Exec wrapper """
    @staticmethod
    def exec_cmd(cmd, stdout_file=None, stderr_file=None, cwd=None, shell=False):
        """ Execute command """
        stdout, stdout_fd = Executor.output_file(stdout_file)
        stderr, stderr_fd = Executor.output_file(stderr_file)

        try:
            proc = subprocess.Popen(cmd,
                                    cwd=cwd,
                                    stdout=stdout,
                                    stderr=stderr,
                                    shell=shell)
            proc.wait()
            out, err = map(lambda s: s.decode().strip('\n\r') if s else '', proc.communicate())

            return proc.returncode, out, err
        except Exception as ex:
            return 1, None, str(ex)
        finally:
            if stderr_fd:
                stderr_fd.close()
            if stdout_fd:
                stdout_fd.close()

    @staticmethod
    def output_file(output_file, default_output=None):
        """ Open output file if specified """
        output_fd = None
        if output_file is not None:
            if output_file == '-':
                output = default_output
            else:
                output_fd = open(output_file, 'w')
                output = output_fd
        else:
            output = subprocess.PIPE
        return output, output_fd

    @staticmethod
    def exec_cmd_only_success(cmd, stdout_file=None, stderr_file=None, cwd=None, shell=False):
        """ Execute command and raise exception if unsuccessfull """
        code, out, err = Executor.exec_cmd(cmd, stdout_file=stdout_file,
                                           stderr_file=stderr_file, cwd=cwd, shell=shell)
        if code != 0:
            raise ExecutorError('Exec command error:\n{}'.format(err))
        return out
