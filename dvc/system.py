import ctypes
import os
import re
import subprocess
from builtins import str

if os.name == 'nt':
    import ntfsutils.hardlink as winlink
    from ntfsutils.fs import *


class System(object):
    SYMLINK_OUTPUT = '<SYMLINK>'
    LONG_PATH_BUFFER_SIZE = 1024

    @staticmethod
    def is_unix():
        return os.name != 'nt'

    @staticmethod
    def hardlink(source, link_name):
        if System.is_unix():
            return os.link(source, link_name)

        return winlink.create(source, link_name)

    @staticmethod
    def _getfileinfo(path):
        # FIXME: current implementation from ntfsutils doesn't support dirs,
        # and until https://github.com/sid0/ntfs/pull/10 is not merged and
        # pip package not updated, we have to use our own version.
        hfile = CreateFile(path, GENERIC_READ, FILE_SHARE_READ, None, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, None)
        if hfile is None:
            raise WinError()
        info = BY_HANDLE_FILE_INFORMATION()
        rv = GetFileInformationByHandle(hfile, info)
        CloseHandle(hfile)
        if rv == 0:
            raise WinError()
        return info

    @staticmethod
    def inode(path):
        if System.is_unix():
            return os.stat(path).st_ino

        info = System._getfileinfo(path)
        return hash((info.dwVolumeSerialNumber, info.nFileIndexHigh, info.nFileIndexLow))

    @staticmethod
    def samefile(path1, path2):
        if not os.path.exists(path1) or not os.path.exists(path2):
            return False

        if System.is_unix():
            return os.path.samefile(path1, path2)

        return winlink.samefile(path1, path2)
