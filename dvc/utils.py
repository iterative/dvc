"""
Helpers for other modules.
"""
import os
import stat
import shutil

from multiprocessing.pool import ThreadPool

from dvc.progress import progress
from dvc.logger import Logger


LOCAL_CHUNK_SIZE = 1024*1024*1024

def cached_property(func):
    '''A decorator for caching properties in classes.'''
    def get(self):
        '''Try obtaining cache'''
        try:
            return self._property_cache[func]
        except AttributeError:
            self._property_cache = {}
            ret = self._property_cache[func] = func(self)
            return ret
        except KeyError:
            ret = self._property_cache[func] = func(self)
            return ret

    return property(get)


def rmtree(directory):
    '''Cross platform rmtree()'''
    if os.name == 'nt':
        if os.path.exists(directory) and not os.access(directory, os.W_OK):
            os.chmod(directory, stat.S_IWUSR)

    shutil.rmtree(directory, ignore_errors=True)


def copyfile(src, dest):
    '''Copy file with progress bar'''
    copied = 0
    name = os.path.basename(src)
    total = os.stat(src).st_size

    fsrc = open(src, 'rb')

    if os.path.isdir(dest):
        fdest = open(dest + '/' + name, 'wb+')
    else:
        fdest = open(dest, 'wb+')

    while True:
        buf = fsrc.read(LOCAL_CHUNK_SIZE)
        if not buf:
            break
        fdest.write(buf)
        copied += len(buf)
        progress.update_target(name, copied, total)

    progress.finish_target(name)

    fsrc.close()
    fdest.close()


def map_progress(func, targets, n_threads):
    """
    Process targets in multi-threaded mode with progress bar
    """
    progress.set_n_total(len(targets))
    pool = ThreadPool(processes=n_threads)
    ret = []

    try:
        ret = pool.map(func, targets)
    except Exception as exc:
        Logger.error('Unexpected exception while processing targets: {}'.format(exc))
    finally:
        progress.finish()

    return zip(targets, ret)
