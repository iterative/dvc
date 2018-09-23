import os

if os.name == 'nt':
    import ntfsutils.hardlink as winlink


class System(object):
    @staticmethod
    def is_unix():
        return os.name != 'nt'

    @staticmethod
    def hardlink(source, link_name):
        if System.is_unix():
            return os.link(source, link_name)

        return winlink.create(source, link_name)

    @staticmethod
    def symlink(source, link_name):
        import ctypes
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
    def _reflink_darwin(src, dst):
        import ctypes

        clib = ctypes.CDLL('libc.dylib')
        if not hasattr(clib, 'clonefile'):
            return -1

        clonefile = clib.clonefile
        clonefile.argtypes = [ctypes.c_char_p,
                              ctypes.c_char_p,
                              ctypes.c_int]
        clonefile.restype = ctypes.c_int

        return clonefile(ctypes.c_char_p(src.encode('utf-8')),
                         ctypes.c_char_p(src.encode('utf-8')),
                         ctypes.c_int(0))

    @staticmethod
    def _reflink_windows(src, dst):
        return -1

    @staticmethod
    def _reflink_linux(src, dst):
        import os
        import fcntl

        FICLONE = 0x40049409

        s = open(src, 'r')
        d = open(dst, 'w+')

        try:
            ret = fcntl.ioctl(d.fileno(), FICLONE, s.fileno())
        except IOError:
            s.close()
            d.close()
            os.unlink(dst)
            raise

        s.close()
        d.close()

        if ret != 0:
            os.unlink(dst)

        return ret

    @staticmethod
    def reflink(source, link_name):
        import platform
        from dvc.exceptions import DvcException

        system = platform.system()
        try:
            if system == 'Windows':
                ret = System._reflink_windows(source, link_name)
            elif system == 'Darwin':
                ret = System._reflink_darwin(source, link_name)
            elif system == 'Linux':
                ret = System._reflink_linux(source, link_name)
            else:
                ret = -1
        except IOError:
            ret = -1

        if ret != 0:
            raise DvcException('Reflink is not supported')

    # FIXME
    # Temporary fix while waiting for the PR to be merged and released:
    # https://github.com/sunshowers/ntfs/pull/11
    @staticmethod
    def getdirinfo(path):
        from ntfsutils.fs import FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_READ
        from ntfsutils.fs import OPEN_EXISTING, BY_HANDLE_FILE_INFORMATION
        from ntfsutils.fs import CreateFile, WinError
        from ntfsutils.fs import GetFileInformationByHandle, CloseHandle
        flags = FILE_FLAG_BACKUP_SEMANTICS
        hfile = CreateFile(path,
                           0,
                           FILE_SHARE_READ,
                           None,
                           OPEN_EXISTING,
                           flags,
                           None)
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
            return os.lstat(path).st_ino

        # getdirinfo from ntfsutils works on both files and dirs
        info = System.getdirinfo(path)
        return hash((info.dwVolumeSerialNumber,
                     info.nFileIndexHigh,
                     info.nFileIndexLow))
