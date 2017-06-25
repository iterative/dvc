import os
import stat
import shutil

from multiprocessing.pool import ThreadPool

from dvc.progress import progress

LOCAL_CHUNK_SIZE = 1024*1024*1024

def cached_property(f):
    def get(self):
        try:
            return self._property_cache[f]
        except AttributeError:
            self._property_cache = {}
            x = self._property_cache[f] = f(self)
            return x
        except KeyError:
            x = self._property_cache[f] = f(self)
            return x

    return property(get)


def rmtree(dir):
    '''Cross platform rmtree()'''
    if os.name == 'nt':
        if os.path.exists(dir) and not os.access(dir, os.W_OK):
            os.chmod(dir, stat.S_IWUSR)

    shutil.rmtree(dir, ignore_errors=True)


def copyfile(src, dest):
    copied = 0
    name = os.path.basename(src)
    total = os.stat(src).st_size

    f = open(src, 'rb')

    if os.path.isdir(dest):
        o = open(dest + '/' + name, 'wb+')
    else:
        o = open(dest, 'wb+')

    while True:
        buf = f.read(LOCAL_CHUNK_SIZE)
        if not buf:
            break
        o.write(buf)
        copied += len(buf)
        progress.update_target(name, copied, total)

    progress.finish_target(name)

    f.close()
    o.close()


def map_progress(func, targets, n_threads):
    """
    Process targets in multi-threaded mode with progress bar
    """
    progress.set_n_total(len(targets))
    p = ThreadPool(processes=n_threads)
    p.map(func, targets)
    progress.finish()
