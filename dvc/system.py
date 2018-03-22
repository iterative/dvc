import ctypes
import os
import re
import subprocess
from builtins import str

if os.name == 'nt':
    import ntfsutils.hardlink as winlink
    from ntfsutils.fs import getdirinfo


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
    def inode(path):
        if System.is_unix():
            return os.stat(path).st_ino

        # getdirinfo from ntfsutils works on both files and dirs
        info = getdirinfo(path)
        return hash((info.dwVolumeSerialNumber, info.nFileIndexHigh, info.nFileIndexLow))

    @staticmethod
    def samefile(path1, path2):
        if not os.path.exists(path1) or not os.path.exists(path2):
            return False

        if System.is_unix():
            return os.path.samefile(path1, path2)

        return winlink.samefile(path1, path2)
