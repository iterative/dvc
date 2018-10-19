import os


class System(object):
    @staticmethod
    def is_unix():
        return os.name != 'nt'

    @staticmethod
    def hardlink(source, link_name):
        import ctypes
        if System.is_unix():
            os.link(source, link_name)
            return

        CreateHardLink = ctypes.windll.kernel32.CreateHardLinkW
        CreateHardLink.argtypes = [ctypes.c_wchar_p,
                                   ctypes.c_wchar_p,
                                   ctypes.c_void_p]
        CreateHardLink.restype = ctypes.wintypes.BOOL

        res = CreateHardLink(link_name, source, None)
        if res == 0:
            raise ctypes.WinError()

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
                         ctypes.c_char_p(dst.encode('utf-8')),
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

    @staticmethod
    def getdirinfo(path):
        import ctypes
        from ctypes import c_void_p, c_wchar_p, Structure, WinError, POINTER
        from ctypes.wintypes import DWORD, HANDLE, BOOL

        FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
        FILE_SHARE_READ = 0x00000001
        OPEN_EXISTING = 3

        class FILETIME(Structure):
            _fields_ = [("dwLowDateTime", DWORD),
                        ("dwHighDateTime", DWORD)]

        class BY_HANDLE_FILE_INFORMATION(Structure):
            _fields_ = [("dwFileAttributes", DWORD),
                        ("ftCreationTime", FILETIME),
                        ("ftLastAccessTime", FILETIME),
                        ("ftLastWriteTime", FILETIME),
                        ("dwVolumeSerialNumber", DWORD),
                        ("nFileSizeHigh", DWORD),
                        ("nFileSizeLow", DWORD),
                        ("nNumberOfLinks", DWORD),
                        ("nFileIndexHigh", DWORD),
                        ("nFileIndexLow", DWORD)]

        flags = FILE_FLAG_BACKUP_SEMANTICS

        func = ctypes.windll.kernel32.CreateFileW
        func.argtypes = [c_wchar_p,
                         DWORD,
                         DWORD,
                         c_void_p,
                         DWORD,
                         DWORD,
                         HANDLE]
        func.restype = HANDLE

        hfile = func(path,
                     0,
                     FILE_SHARE_READ,
                     None,
                     OPEN_EXISTING,
                     flags,
                     None)
        if hfile is None:
            raise WinError()

        func = ctypes.windll.kernel32.GetFileInformationByHandle
        func.argtypes = [HANDLE, POINTER(BY_HANDLE_FILE_INFORMATION)]
        func.restype = BOOL

        info = BY_HANDLE_FILE_INFORMATION()
        rv = func(hfile, info)

        func = ctypes.windll.kernel32.CloseHandle
        func.argtypes = [HANDLE]
        func.restype = BOOL

        func(hfile)

        if rv == 0:
            raise WinError()

        return info

    @staticmethod
    def inode(path):
        if System.is_unix():
            import ctypes
            inode = os.lstat(path).st_ino
            # NOTE: See https://bugs.python.org/issue29619 and
            # https://stackoverflow.com/questions/34643289/
            # pythons-os-stat-is-returning-wrong-inode-value
            inode = ctypes.c_ulong(inode).value
        else:
            # getdirinfo from ntfsutils works on both files and dirs
            info = System.getdirinfo(path)
            inode = abs(hash((info.dwVolumeSerialNumber,
                              info.nFileIndexHigh,
                              info.nFileIndexLow)))
        assert inode >= 0
        assert inode < 2**64
        return inode
