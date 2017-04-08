import os
import sys
import ctypes

from dvc.config import Config, ConfigI
from dvc.exceptions import DvcException
from dvc.executor import Executor
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

        Runtime._setup_windows_symlink()

        os.symlink = Runtime.symlink
        os.path.islink = Runtime.is_link

    @staticmethod
    def _setup_windows_symlink():
        func = ctypes.windll.kernel32.CreateSymbolicLinkW
        func.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
        func.restype = ctypes.c_ubyte
        Runtime.SYMLINC_OVERRIDE = func

    @staticmethod
    def is_link(path):
        if not os.path.exists(path):
            return False

        if not os.path.isfile(path):
            return False

        # It is definitely not the best way to check a symlink.
        output = Executor.exec_cmd_only_success(["dir", path])
        return '<SYMLINK>' in output

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
