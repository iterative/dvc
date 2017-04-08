import os
import ctypes


class WindowsSymlinkOverride(object):
    FUNC = None

    def __init__(self):
        func = ctypes.windll.kernel32.CreateSymbolicLinkW
        func.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
        func.restype = ctypes.c_ubyte

        WindowsSymlinkOverride.FUNC = func

    @staticmethod
    def symlink(source, link_name):
        '''symlink(source, link_name) - DVC override for Windows
           Creates a symbolic link pointing to source named link_name'''

        flags = 0
        if source is not None and os.path.isdir(source):
            flags = 1
        if WindowsSymlinkOverride.FUNC(link_name, source, flags) == 0:
            raise ctypes.WinError()


def setup():
    if os.name == 'nt':
        os.symlink = WindowsSymlinkOverride().symlink
    pass
