import errno
import logging
import os
import platform

from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class System:
    @staticmethod
    def hardlink(source, link_name):
        try:
            os.link(source, link_name)
        except OSError as exc:
            raise DvcException("failed to link") from exc

    @staticmethod
    def symlink(source, link_name):
        try:
            os.symlink(source, link_name)
        except OSError as exc:
            raise DvcException("failed to symlink") from exc

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
            ctypes.c_char_p(os.fsencode(src)),
            ctypes.c_char_p(os.fsencode(dst)),
            ctypes.c_int(0),
        )

    @staticmethod
    def _reflink_windows(_src, _dst):
        return -1

    @staticmethod
    def _reflink_linux(src, dst):
        import fcntl

        FICLONE = 0x40049409

        try:
            ret = 255
            with open(src) as s, open(dst, "w+") as d:
                ret = fcntl.ioctl(d.fileno(), FICLONE, s.fileno())
        finally:
            if ret != 0:
                os.unlink(dst)

        return ret

    @staticmethod
    def reflink(source, link_name):
        from dvc.utils.fs import umask

        source, link_name = os.fspath(source), os.fspath(link_name)

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
        except OSError:
            ret = -1

        if ret != 0:
            raise DvcException("reflink is not supported")

        # NOTE: reflink has a new inode, but has the same mode as the src,
        # so we need to chmod it to look like a normal copy.
        os.chmod(link_name, 0o666 & ~umask)

    @staticmethod
    def inode(path):
        return os.lstat(path).st_ino

    @staticmethod
    def is_symlink(path):
        return os.path.islink(path)

    @staticmethod
    def is_hardlink(path):
        return os.stat(path).st_nlink > 1
