import os
import ntpath
import sys
import json
import time

from dvc.exceptions import DvcException
from dvc.path.data_item import NotInDataDirError
from dvc.system import System


class StateFileError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'State file error: {}'.format(msg))


class StateFile(object):
    MAGIC = 'DVC-State'
    VERSION = '0.1'

    DVC_PYTHON_FILE_NAME = 'dvc.py'
    DVC_COMMAND = 'dvc'

    COMMAND_RUN = 'run'
    COMMAND_IMPORT_FILE = 'import-file'
    COMMAND_EMPTY_FILE = 'empty'
    ACCEPTED_COMMANDS = {COMMAND_IMPORT_FILE, COMMAND_RUN, COMMAND_EMPTY_FILE}

    PARAM_COMMAND = 'Command'
    PARAM_TYPE = 'Type'
    PARAM_VERSION = 'Version'
    PARAM_ARGV = 'Argv'
    PARAM_CWD = 'Cwd'
    PARAM_CREATED_AT = 'CreatedAt'
    PARAM_INPUT_FILES = 'InputFiles'
    PARAM_OUTPUT_FILES = 'OutputFiles'
    PARAM_CODE_DEPENDENCIES = 'CodeDependencies'
    PARAM_LOCKED = 'Locked'
    PARAM_STDOUT = "Stdout"
    PARAM_STDERR = "Stderr"
    PARAM_SHELL = "Shell"

    def __init__(self,
                 command,
                 data_item,
                 settings,
                 input_files,
                 output_files,
                 code_dependencies=[],
                 lock=False,
                 argv=sys.argv,
                 stdout=None,
                 stderr=None,
                 created_at=time.strftime('%Y-%m-%d %H:%M:%S %z'),
                 cwd=None,
                 shell=False):
        self.data_item = data_item
        self.file = data_item.state.relative
        self.settings = settings
        self.input_files = input_files
        self.output_files = output_files
        self.locked = lock
        self.code_dependencies = code_dependencies
        self.shell = shell

        if command not in self.ACCEPTED_COMMANDS:
            raise StateFileError('Args error: unknown command %s' % command)
        self.command = command

        self._argv = argv

        self.stdout = stdout
        self.stderr = stderr

        self.created_at = created_at

        if cwd:
            self.cwd = cwd
        else:
            self.cwd = self.get_dvc_path()
        pass

    @property
    def is_import_file(self):
        return self.command == self.COMMAND_IMPORT_FILE

    @property
    def is_run(self):
        return self.command == self.COMMAND_RUN

    @property
    def argv(self):
        return self._argv

    @staticmethod
    def _replace_paths(l, old, new):
        if os.path != ntpath:
            return l

        ret = []
        for x in l:
            if x == None:
                ret.append(None)
                continue

            ret.append(x.replace(old, new))

        return ret

    @staticmethod
    def decode_paths(l):
        return StateFile._replace_paths(l, '/', '\\')

    @staticmethod
    def decode_path(p):
        return StateFile.decode_paths([p])[0]

    @staticmethod
    def encode_paths(l):
        return StateFile._replace_paths(l, '\\', '/')

    @staticmethod
    def encode_path(p):
        return StateFile.encode_paths([p])[0]

    @staticmethod
    def load(data_item, settings):
        with open(data_item.state.relative, 'r') as fd:
            data = json.load(fd)

        return StateFile(StateFile.decode_path(data.get(StateFile.PARAM_COMMAND)),
                         data_item,
                         settings,
                         StateFile.decode_paths(data.get(StateFile.PARAM_INPUT_FILES, [])),
                         StateFile.decode_paths(data.get(StateFile.PARAM_OUTPUT_FILES, [])),
                         StateFile.decode_paths(data.get(StateFile.PARAM_CODE_DEPENDENCIES, [])),
                         data.get(StateFile.PARAM_LOCKED, False),
                         StateFile.decode_paths(data.get(StateFile.PARAM_ARGV)),
                         StateFile.decode_path(data.get(StateFile.PARAM_STDOUT)),
                         StateFile.decode_path(data.get(StateFile.PARAM_STDERR)),
                         data.get(StateFile.PARAM_CREATED_AT),
                         StateFile.decode_path(data.get(StateFile.PARAM_CWD)),
                         data.get(StateFile.PARAM_SHELL, False))

    def save(self):
        argv = self._argv_paths_normalization(self._argv)

        res = {
            self.PARAM_COMMAND:             self.encode_path(self.command),
            self.PARAM_TYPE:                self.MAGIC,
            self.PARAM_VERSION:             self.VERSION,
            self.PARAM_ARGV:                self.encode_paths(argv),
            self.PARAM_CWD:                 self.encode_path(self.cwd),
            self.PARAM_CREATED_AT:          self.created_at,
            self.PARAM_INPUT_FILES:         self.encode_paths(self.input_files),
            self.PARAM_OUTPUT_FILES:        self.encode_paths(self.output_files),
            self.PARAM_CODE_DEPENDENCIES:   self.encode_paths(self.code_dependencies),
            self.PARAM_STDOUT:              self.encode_path(self.stdout),
            self.PARAM_STDERR:              self.encode_path(self.stderr),
            self.PARAM_SHELL:               self.shell,
        }

        if self.locked:
            res[self.PARAM_LOCKED] = True

        file_dir = os.path.dirname(self.file)
        if file_dir != '' and not os.path.isdir(file_dir):
            os.makedirs(file_dir)

        with open(self.file, 'w') as fd:
            json.dump(res, fd, indent=2)
        pass

    def _argv_paths_normalization(self, argv):
        result = []

        for arg in argv:
            try:
                data_item = self.settings.path_factory.data_item(arg)
                result.append(data_item.data.dvc)
            except NotInDataDirError:
                result.append(arg)

        return result

    def get_dvc_path(self):
        pwd = System.get_cwd()
        if not pwd.startswith(self.settings.git.git_dir_abs):
            raise StateFileError('the file cannot be created outside of a git repository')

        return os.path.relpath(pwd, self.settings.git.git_dir_abs)
