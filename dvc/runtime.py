import os
import sys
import ctypes

from dvc.config import Config, ConfigI
from dvc.exceptions import DvcException
from dvc.git_wrapper import GitWrapper
from dvc.logger import Logger
from dvc.settings import Settings


class Runtime(object):
    CONFIG = 'dvc.conf'
    SYMLINC_OVERRIDE = None

    @staticmethod
    def symlink_setup():
        if os.name != 'nt':
            return

        func = ctypes.windll.kernel32.CreateSymbolicLinkW
        func.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
        func.restype = ctypes.c_ubyte

        Runtime.SYMLINC_OVERRIDE = func

        os.symlink = Runtime.symlink

    @staticmethod
    def symlink(source, link_name):
        '''symlink(source, link_name) - DVC override for Windows
           Creates a symbolic link pointing to source named link_name'''

        flags = 0
        if source is not None and os.path.isdir(source):
            flags = 1
        if Runtime.SYMLINC_OVERRIDE(link_name, source, flags) == 0:
            raise ctypes.WinError()

    @staticmethod
    def conf_file_path(git_dir):
        return os.path.realpath(os.path.join(git_dir, Runtime.CONFIG))

    @staticmethod
    def run(cmd_class, parse_config=True):
        try:
            Runtime.symlink_setup()
            runtime_git = GitWrapper()

            if parse_config:
                runtime_config = Config(Runtime.conf_file_path(runtime_git.git_dir))
            else:
                runtime_config = ConfigI()

            args = sys.argv[1:]

            instance = cmd_class(Settings(args, runtime_git, runtime_config))
            sys.exit(instance.run())
        except DvcException as e:
            Logger.error(e)
            sys.exit(1)
