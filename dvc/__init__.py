import os

__CSL = None


def win_symlink(source, link_name):
    '''symlink(source, link_name) - DVC override for Windows
       Creates a symbolic link pointing to source named link_name'''

    flags = 0
    if source is not None and os.path.isdir(source):
        flags = 1
    if __CSL(link_name, source, flags) == 0:
        raise ctypes.WinError()


def setup():
    if os.name == 'nt':
        global __CSL
        if __CSL is None:
            import ctypes
            csl = ctypes.windll.kernel32.CreateSymbolicLinkW
            csl.argtypes = (ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32)
            csl.restype = ctypes.c_ubyte
            __CSL = csl

        os.symlink = win_symlink
    pass
