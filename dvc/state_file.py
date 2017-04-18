import os
import sys
import json
import time

from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.system import System


class StateFileError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'State file error: {}'.format(msg))


class StateFile(object):
    MAGIC = 'DVC-State'
    VERSION = '0.1'

    PARAM_TYPE = 'Type'
    PARAM_VERSION = 'Version'
    PARAM_ARGV = 'Argv'
    PARAM_NORM_ARGV = 'NormArgv'
    PARAM_CWD = 'Cwd'
    PARAM_CREATED_AT = 'CreatedAt'
    PARAM_INPUT_FILES = 'InputFiles'
    PARAM_OUTPUT_FILES = 'OutputFiles'
    PARAM_CODE_DEPENDENCIES = 'CodeDependencies'
    PARAM_NOT_REPRODUCIBLE = 'NotReproducible'
    PARAM_STDOUT = "Stdout"
    PARAM_STDERR = "Stderr"

    def __init__(self, file, git, input_files, output_files,
                 code_dependencies=[],
                 is_reproducible=True,
                 argv=sys.argv,
                 stdout=None,
                 stderr=None,
                 norm_argv=None,
                 created_at=time.strftime('%Y-%m-%d %H:%M:%S %z'),
                 cwd=None):
        self.file = file
        self.git = git
        self.input_files = input_files
        self.output_files = output_files
        self.is_reproducible = is_reproducible
        self.code_dependencies = code_dependencies

        self.argv = argv
        if norm_argv:
            self.norm_argv = norm_argv
        else:
            self.norm_argv = self.normalized_args()

        self.stdout = stdout
        self.stderr = stderr

        self.created_at = created_at

        if cwd:
            self.cwd = cwd
        else:
            self.cwd = self.get_dvc_path()
        pass

    @staticmethod
    def load(filename, git):
        with open(filename, 'r') as fd:
            data = json.load(fd)

        return StateFile(filename,
                         git,
                         data.get(StateFile.PARAM_INPUT_FILES, []),
                         data.get(StateFile.PARAM_OUTPUT_FILES, []),
                         data.get(StateFile.PARAM_CODE_DEPENDENCIES, []),
                         not data.get(StateFile.PARAM_NOT_REPRODUCIBLE, False),
                         data.get(StateFile.PARAM_ARGV),
                         data.get(StateFile.PARAM_STDOUT),
                         data.get(StateFile.PARAM_STDERR),
                         data.get(StateFile.PARAM_NORM_ARGV),
                         data.get(StateFile.PARAM_CREATED_AT),
                         data.get(StateFile.PARAM_CWD))

    def save(self):
        res = {
            self.PARAM_TYPE:            self.MAGIC,
            self.PARAM_VERSION:         self.VERSION,
            self.PARAM_ARGV:            self.process_args(self.argv, 'argv'),
            self.PARAM_NORM_ARGV:       self.process_args(self.norm_argv, 'normalized argv'),
            self.PARAM_CWD:             self.cwd,
            self.PARAM_CREATED_AT:      self.created_at,
            self.PARAM_INPUT_FILES:     self.input_files,
            self.PARAM_OUTPUT_FILES:    self.output_files,
            self.PARAM_CODE_DEPENDENCIES:   self.code_dependencies,
            self.PARAM_STDOUT:          self.stdout,
            self.PARAM_STDERR:          self.stderr
        }

        if not self.is_reproducible:
            res[self.PARAM_NOT_REPRODUCIBLE] = True

        file_dir = os.path.dirname(self.file)
        if file_dir != '' and not os.path.isdir(file_dir):
            os.makedirs(file_dir)

        with open(self.file, 'w') as fd:
            json.dump(res, fd, indent=2)
        pass

    def process_args(self, argv, name='argv'):
        was_changed = False
        result = []

        for arg in argv:
            if arg.endswith('dvc2.py'):
                result.append('dvc')
                was_changed = True
            else:
                result.append(arg)

        if was_changed:
            Logger.debug('Save state file {}. Replace {} "{}" to "{}"'.format(
                self.file,
                name,
                argv,
                result
            ))

        return result

    def normalized_args(self):
        result = []

        if len(self.argv) > 0:
            cmd = self.argv[0]
            pos = cmd.rfind(os.sep)
            if pos >= 0:
                cmd = cmd[pos+1:]
            result.append(cmd)

            for arg in self.argv[1:]:
                if os.path.isfile(arg):     # CHANGE to data items
                    path = os.path.abspath(arg)
                    dvc_path = os.path.relpath(path, self.git.git_dir_abs)
                    result.append(dvc_path)
                else:
                    result.append(arg)

        return result

    def get_dvc_path(self):
        pwd = System.get_cwd()
        if not pwd.startswith(self.git.git_dir_abs):
            raise StateFileError('the file cannot be created outside of a git repository')

        return os.path.relpath(pwd, self.git.git_dir_abs)
