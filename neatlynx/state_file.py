import os
import sys
import json
import time

from neatlynx.exceptions import NeatLynxException
from neatlynx.git_wrapper import GitWrapper


class StateFileError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'State file error: {}'.format(msg))


class StateFile(object):
    MAGIC = 'NLX-State'
    VERSION = '0.1'

    def __init__(self, file, git):
        self.file = file
        self.git = git

    def save(self):
        res = {
            'Type': self.MAGIC,
            'Version': self.VERSION,
            'Argv': sys.argv,
            'NLX_cwd': self.get_nlx_path(),
            'CreatedAt': time.strftime('%Y-%m-%d %H:%M:%S %z')
        }

        file_dir = os.path.dirname(self.file)
        if file_dir != '':
            os.makedirs(file_dir, exist_ok=True)

        with open(self.file, 'w') as fd:
            json.dump(res, fd, indent=2)
        pass

    def get_nlx_path(self):
        pwd = os.path.realpath(os.curdir)
        if not pwd.startswith(self.git.git_dir_abs):
            raise StateFileError('the file cannot be created outside of a git repository')

        return os.path.relpath(pwd, self.git.git_dir_abs)

    def repro(self):
        with open(self.file, 'r') as fd:
            res = json.load(fd)

        argv = res['Argv']
        argv.insert(0, 'python')
        argv.insert(2, '--ignore-git-status')

        cwd = os.path.join(self.git.git_dir_abs, res['NLX_cwd'])
        return GitWrapper.exec_cmd(argv, cwd=cwd)
