from __future__ import unicode_literals

import errno
import logging
import os
import shutil

from dvc.utils.compat import fspath
from dvc.utils.compat import open
from dvc.utils.compat import str


logger = logging.getLogger(__name__)


class System(object):
    @staticmethod
    def is_unix():
        return os.name != "nt"

    @staticmethod
    def copy(src, dest):
        src, dest = fspath(src), fspath(dest)
        return shutil.copyfile(src, dest)

    @staticmethod
    def hardlink(source, link_name):
        import ctypes
        from dvc.exceptions import DvcException

        source, link_name = fspath(source), fspath(link_name)

        if System.is_unix():
            try:
                os.link(source, link_name)
                return
            except Exception as exc:
                raise DvcException("link", cause=exc)

        CreateHardLink = ctypes.windll.kernel32.CreateHardLinkW
        CreateHardLink.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_wchar_p,
            ctypes.c_void_p,
        ]
        CreateHardLink.restype = ctypes.wintypes.BOOL

        res = CreateHardLink(link_name, source, None)
        if res == 0:
            raise DvcException("CreateHardLinkW", cause=ctypes.WinError())

    @staticmethod
    def symlink(source, link_name):
        import ctypes
        from dvc.exceptions import DvcException

        source, link_name = fspath(source), fspath(link_name)

        if System.is_unix():
            try:
                os.symlink(source, link_name)
                return
            except Exception as exc:
                msg = "failed to symlink '{}' -> '{}': {}"
                raise DvcException(msg.format(source, link_name, str(exc)))

        flags = 0
        if source is not None and os.path.isdir(source):
            flags = 1

        func = ctypes.windll.kernel32.CreateSymbolicLinkW
        func.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
        func.restype = ctypes.c_ubyte

        if func(link_name, source, flags) == 0:
            raise DvcException("CreateSymbolicLinkW", cause=ctypes.WinError())

    @staticmethod
    def _reflink_darwin(src, dst):
        import ctypes

        LIBC = "libc.dylib"
        LIBC_FALLBACK = "/usr/lib/libSystem.dylib"
        try:
            clib = ctypes.CDLL(LIBC)
        except OSError as exc:
            logger.debug(
                "unable to access '{}' (errno '{}'). "
                "Falling back to '{}'.".format(LIBC, exc.errno, LIBC_FALLBACK)
            )
            if exc.errno != errno.ENOENT:
                raise
            # NOTE: trying to bypass System Integrity Protection (SIP)
            clib = ctypes.CDLL(LIBC_FALLBACK)

        if not hasattr(clib, "clonefile"):
            return -1

        clonefile = clib.clonefile
        clonefile.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int]
        clonefile.restype = ctypes.c_int

        return clonefile(
            ctypes.c_char_p(src.encode("utf-8")),
            ctypes.c_char_p(dst.encode("utf-8")),
            ctypes.c_int(0),
        )

    @staticmethod
    def _reflink_windows(src, dst):
        return -1

    @staticmethod
    def _reflink_linux(src, dst):
        import os
        import fcntl

        FICLONE = 0x40049409

        try:
            ret = 255
            with open(src, "r") as s, open(dst, "w+") as d:
                ret = fcntl.ioctl(d.fileno(), FICLONE, s.fileno())
        finally:
            if ret != 0:
                os.unlink(dst)

        return ret

    @staticmethod
    def reflink(source, link_name):
        import platform
        from dvc.exceptions import DvcException

        source, link_name = fspath(source), fspath(link_name)

        system = platform.system()
        try:
            if system == "Windows":
                ret = System._reflink_windows(source, link_name)
            elif system == "Darwin":
                ret = System._reflink_darwin(source, link_name)
            elif system == "Linux":
                ret = System._reflink_linux(source, link_name)
            else:
                ret = -1
        except IOError:
            ret = -1

        if ret != 0:
            raise DvcException("reflink is not supported")

    @staticmethod
    def _getdirinfo(path):
        from collections import namedtuple
        from win32file import (
            CreateFileW,
            GetFileInformationByHandle,
            FILE_FLAG_BACKUP_SEMANTICS,
            FILE_FLAG_OPEN_REPARSE_POINT,
            FILE_SHARE_READ,
            OPEN_EXISTING,
        )

        # NOTE: use FILE_FLAG_OPEN_REPARSE_POINT to open symlink itself and not
        # the target See https://docs.microsoft.com/en-us/windows/desktop/api/
        # fileapi/nf-fileapi-createfilew#symbolic-link-behavior
        flags = FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT

        hfile = CreateFileW(
            path, 0, FILE_SHARE_READ, None, OPEN_EXISTING, flags, None
        )

        # See BY_HANDLE_FILE_INFORMATION structure from fileapi.h
        Info = namedtuple(
            "BY_HANDLE_FILE_INFORMATION",
            [
                "dwFileAttributes",
                "ftCreationTime",
                "ftLastAccessTime",
                "ftLastWriteTime",
                "dwVolumeSerialNumber",
                "nFileSizeHigh",
                "nFileSizeLow",
                "nNumberOfLinks",
                "nFileIndexHigh",
                "nFileIndexLow",
            ],
        )

        return Info(*GetFileInformationByHandle(hfile))

    @staticmethod
    def inode(path):
        path = fspath(path)

        if System.is_unix():
            import ctypes

            inode = os.lstat(path).st_ino
            # NOTE: See https://bugs.python.org/issue29619 and
            # https://stackoverflow.com/questions/34643289/
            # pythons-os-stat-is-returning-wrong-inode-value
            inode = ctypes.c_ulong(inode).value
        else:
            # getdirinfo from ntfsutils works on both files and dirs
            info = System._getdirinfo(path)
            inode = abs(
                hash(
                    (
                        info.dwVolumeSerialNumber,
                        info.nFileIndexHigh,
                        info.nFileIndexLow,
                    )
                )
            )
        assert inode >= 0
        assert inode < 2 ** 64
        return inode

    @staticmethod
    def is_symlink(path):
        path = fspath(path)

        if System.is_unix():
            return os.path.islink(path)

        # https://docs.microsoft.com/en-us/windows/desktop/fileio/
        # file-attribute-constants
        from winnt import FILE_ATTRIBUTE_REPARSE_POINT

        if os.path.lexists(path):
            info = System._getdirinfo(path)
            return info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT
        return False

    @staticmethod
    def is_hardlink(path):
        path = fspath(path)

        if System.is_unix():
            return os.stat(path).st_nlink > 1

        info = System._getdirinfo(path)
        return info.nNumberOfLinks > 1
