"""
Helpers for other modules.
"""
import os
import re
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
        Logger.error('Unexpected exception while processing targets: {}'.format(exc), exc_info=True)
    finally:
        progress.finish()

    return list(zip(targets, ret))


def parse_target_metric_file(file_name):
    with open(file_name, 'r') as fd:
        try:
            lines = fd.readlines(2)
        except Exception:
            return None
        return parse_target_metric(lines)


FLOATS_FROM_STRING = re.compile(r'[-+]?(?:(?:\d*\.\d+)|(?:\d+\.?))(?:[Ee][+-]?\d+)?')


def parse_target_metric(lines):
    if len(lines) != 1:
        return None

    # Extract float from string. I.e. from 'AUC: 0.596182'
    nums = FLOATS_FROM_STRING.findall(lines[0])
    if len(nums) < 1:
        return None

    return float(nums[0])
