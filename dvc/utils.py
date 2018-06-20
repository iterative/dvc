"""
Helpers for other modules.
"""
import os
import re
import sys
import json
import stat
import shutil
import hashlib
from binaryornot.check import is_binary
from multiprocessing.pool import ThreadPool

from dvc.progress import progress
from dvc.logger import Logger
from dvc.binary_file_extensions import BINARY_FILE_EXTENSIONS


LOCAL_CHUNK_SIZE = 1024*1024


def dos2unix(data):
    return data.replace(b'\r\n', b'\n')


def file_md5(fname):
    """ get the (md5 hexdigest, md5 digest) of a file """
    if os.path.exists(fname):
        hash_md5 = hashlib.md5()
        # when dealing with large collections of binary files, the is_binary
        # call becomes a bottleneck, here we first check the extension to
        # avoid this bottleneck when possible:
        if fname.split('.')[-1] in BINARY_FILE_EXTENSIONS:
            binary = True
        else:
            binary = is_binary(fname)

        with open(fname, 'rb') as fobj:
            while True:
                data = fobj.read(LOCAL_CHUNK_SIZE)
                if not data:
                    break

                if binary:
                    chunk = data
                else:
                    chunk = dos2unix(data)

                hash_md5.update(chunk)

        return (hash_md5.hexdigest(), hash_md5.digest())
    else:
        return (None, None)


def bytes_md5(byts):
    hasher = hashlib.md5()
    hasher.update(byts)
    return hasher.hexdigest()


def dict_md5(d):
    byts = json.dumps(d, sort_keys=True).encode('utf-8')
    return bytes_md5(byts)


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


def copyfile(src, dest, no_progress_bar=False, name=None):
    '''Copy file with progress bar'''
    copied = 0
    name = name if name else os.path.basename(dest)
    total = os.stat(src).st_size

    fsrc = open(src, 'rb')

    if os.path.isdir(dest):
        fdest = open(dest + '/' + os.path.basename(src), 'wb+')
    else:
        fdest = open(dest, 'wb+')

    while True:
        buf = fsrc.read(LOCAL_CHUNK_SIZE)
        if not buf:
            break
        fdest.write(buf)
        copied += len(buf)
        if not no_progress_bar:
            progress.update_target(name, copied, total)

    if not no_progress_bar:
        progress.finish_target(name)

    fsrc.close()
    fdest.close()


def wrap(func, t):
    try:
        return func(t)
    except Exception as exc:
        Logger.error('Error', exc)
        raise


def map_progress(func, targets, n_threads):
    """
    Process targets in multi-threaded mode with progress bar
    """
    progress.set_n_total(len(targets))
    pool = ThreadPool(processes=n_threads)
    ret = []

    wrapper = lambda t: wrap(func, t)

    try:
        ret = pool.map(wrapper, targets)
    except Exception as exc:
        raise

    return list(zip(targets, ret))


def move(src, dst):
    dname = os.path.dirname(dst)
    if not os.path.exists(dname):
        os.makedirs(dname)

    shutil.move(src, dst)


def remove(path):
    if not os.path.exists(path):
        return

    Logger.debug(u'Removing \'{}\''.format(os.path.relpath(path)))
    if os.path.isfile(path):
        os.unlink(path)
    else:
        shutil.rmtree(path)
