import os
import sys
import json
import time

from neatlynx.exceptions import NeatLynxException
from neatlynx.git_wrapper import GitWrapper
from neatlynx.logger import Logger


class StateFileError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'State file error: {}'.format(msg))


class StateFile(object):
    MAGIC = 'NLX-State'
    VERSION = '0.1'

    PARAM_TYPE = 'Type'
    PARAM_VERSION = 'Version'
    PARAM_ARGV = 'Argv'
    PARAM_NORM_ARGV = 'NormArgv'
    PARAM_CWD = 'Cwd'
    PARAM_CREATED_AT = 'CreatedAt'
    PARAM_INPUT_FILES = 'InputFiles'
    PARAM_OUTPUT_FILES = 'OutputFiles'

    def __init__(self, file, git, input_files, output_files,
                 argv=sys.argv, norm_argv=None,
                 created_at=time.strftime('%Y-%m-%d %H:%M:%S %z'),
                 cwd=None):
        self.file = file
        self.git = git
        self.input_files = input_files
        self.output_files = output_files
        self.argv = argv
        if norm_argv:
            self.norm_argv = norm_argv
        else:
            self.norm_argv = self.normalized_args()

        self.created_at = created_at

        if cwd:
            self.cwd = cwd
        else:
            self.cwd = self.get_nlx_path()
        pass

    @staticmethod
    def load(filename, git):
        with open(filename, 'r') as fd:
            data = json.load(fd)

        return StateFile(filename,
                         git,
                         data.get('InputFiles'),
                         data.get('OutputFiles'),
                         data.get('Argv'),
                         data.get('NormArgv'),
                         data.get('CreatedAt'),
                         data.get('Cwd'))

    def save(self):
        res = {
            self.PARAM_TYPE:        self.MAGIC,
            self.PARAM_VERSION:     self.VERSION,
            self.PARAM_ARGV:        self.argv,
            self.PARAM_NORM_ARGV:   self.norm_argv,
            self.PARAM_CWD:         self.cwd,
            self.PARAM_CREATED_AT:  self.created_at,
            self.PARAM_INPUT_FILES: self.input_files,
            self.PARAM_OUTPUT_FILES:self.output_files
        }

        file_dir = os.path.dirname(self.file)
        if file_dir != '':
            os.makedirs(file_dir, exist_ok=True)

        with open(self.file, 'w') as fd:
            json.dump(res, fd, indent=2)
        pass

    def normalized_args(self):
        result = []

        if len(sys.argv) > 0:
            cmd = sys.argv[0]
            pos = cmd.rfind(os.sep)
            if pos >= 0:
                cmd = cmd[pos+1:]
            result.append(cmd)

            for arg in sys.argv[1:]:
                if os.path.isfile(arg):
                    path = os.path.abspath(arg)
                    nlx_path = os.path.relpath(path, self.git.git_dir_abs)
                    result.append(nlx_path)
                else:
                    result.append(arg)

        return result

    def get_nlx_path(self):
        pwd = os.path.realpath(os.curdir)
        if not pwd.startswith(self.git.git_dir_abs):
            raise StateFileError('the file cannot be created outside of a git repository')

        return os.path.relpath(pwd, self.git.git_dir_abs)

    # def repro(self):
    #     with open(self.file, 'r') as fd:
    #         res = json.load(fd)
    #
    #     args = res['NormArgs']
    #     if not args:
    #         raise StateFileError('Error: parameter NormalizedArgs is nor defined in state file "{}"'.
    #                              format(self.file))
    #     if len(args) < 2:
    #         raise StateFileError('Error: reproducible command in state file "{}" is too short'.
    #                              format(self.file))
    #
    #     if args[0][-3:] != '.py':
    #         raise StateFileError('Error: reproducible command format error in state file "{}"'.
    #                              format(self.file))
    #     args.pop(0)
    #
    #     Logger.info("Repro cmd:\n\t{}".format(' '.join(args)))
    #     return GitWrapper.exec_cmd(args, cwd=self.git.git_dir_abs)
