import platform
from cffi import FFI


WINDOWS_SRC = """
int reflink(char *src, char *dst)
{
    return -1;
}
"""

DARWIN_SRC = """
#include <AvailabilityMacros.h>
/* NOTE: sys/clonefile.h is available since OS X 10.12 */
#if MAC_OS_X_VERSION_MIN_REQUIRED >= MAC_OS_X_VERSION_10_12
#include <sys/clonefile.h> /* for clonefile(2) */
#include <errno.h>
int reflink(char *src, char *dst)
{
    return clonefile(src, dst, 0);
}
#else
int reflink(char *src, char *dst)
{
    return -1;
}
#endif
"""

LINUX_SRC = """
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <assert.h>
#include <errno.h>
int reflink(char *src, char *dst)
{
    int ret = -1;
#ifdef FICLONE
    int src_fd = -1;
    int dst_fd = -1;

    src_fd = open(src, O_RDONLY);
    if (src_fd < 0)
        return ret;

    dst_fd = open(dst, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    if (src_fd < 0) {
        close(src_fd);
        return ret;
    }

    ret = ioctl(dst_fd, FICLONE, src_fd);
    if (ret != 0)
        unlink(dst);

out:
    if (src_fd >= 0)
        close(src_fd);
    if (dst_fd >= 0)
        close(dst_fd);
#endif
    return ret;
}
"""

ffibuilder = FFI()

ffibuilder.cdef("""
int reflink(char *src, char *dst);
""")

system = platform.system()
if system == 'Windows':
    src = WINDOWS_SRC
elif system == 'Linux':
    src = LINUX_SRC
elif system == 'Darwin':
    src = DARWIN_SRC
else:
    raise NotImplementedError

ffibuilder.set_source("reflink", src)

if __file__ == '__main__':
    ffibuilder.compile(verbose=True)
