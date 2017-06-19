import ctypes
import os
import re
from builtins import str

if os.name == 'nt':
    from ctypes import create_unicode_buffer, windll

from dvc.executor import Executor


class System(object):
    SYMLINK_OUTPUT = '<SYMLINK>'
    LONG_PATH_BUFFER_SIZE = 1024

    @staticmethod
    def is_unix():
        return os.name != 'nt'

    @staticmethod
    def symlink(source, link_name):
        if System.is_unix():
            return os.symlink(source, link_name)

        flags = 0
        if source is not None and os.path.isdir(source):
            flags = 1

        func = ctypes.windll.kernel32.CreateSymbolicLinkW
        func.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
        func.restype = ctypes.c_ubyte

        if func(link_name, source, flags) == 0:
            raise ctypes.WinError()

    @staticmethod
    def islink(path):
        # It is definitely not the best way to check a symlink.

        if System.is_unix():
            return os.path.islink(path)

        if not os.path.exists(path):
            return False

        if not os.path.isfile(path):
            return False

        return System._get_symlink_string(path) is not None

    @staticmethod
    def _get_symlink_string(path):
        code, output, _ = Executor.exec_cmd(["dir", path], shell=True)
        if code != 0:
            return None

        lines = output.split('\n')
        for line in lines:
            if System.SYMLINK_OUTPUT in line:
                return line
        return None

    @staticmethod
    def realpath(path):
        # It is definitely not the best way to check a symlink.

        if System.is_unix():
            return os.path.realpath(path)

        output = System._get_symlink_string(path)
        if output is None:
            return os.path.realpath(path)

        groups = re.compile(r'\[\S+\]$').findall(output.strip())
        if len(groups) < 1:
            return os.path.realpath(path)

        resolved_link = groups[0][1:-1]
        return resolved_link

    @staticmethod
    def get_long_path(path):
        """Convert short path to a full path. It is needed for Windows."""
        if System.is_unix():
            return path

        buffer = create_unicode_buffer(System.LONG_PATH_BUFFER_SIZE)
        get_long_path_name = windll.kernel32.GetLongPathNameW
        result = get_long_path_name(u'%s' % str(path), buffer, System.LONG_PATH_BUFFER_SIZE)
        if result == 0 or result > System.LONG_PATH_BUFFER_SIZE:
            return path
        return buffer.value

    @staticmethod
    def get_cwd():
        return System.get_long_path(os.getcwd())
