"""
Helpers for other modules.
"""
import os
import sys
import stat
import math
import json
import shutil
import hashlib

from dvc.progress import progress
from dvc.istextfile import istextfile
from dvc.logger import Logger


LOCAL_CHUNK_SIZE = 1024*1024
LARGE_FILE_SIZE = 1024*1024*1024
LARGE_DIR_SIZE = 100


def dos2unix(data):
    return data.replace(b'\r\n', b'\n')


def file_md5(fname):
    """ get the (md5 hexdigest, md5 digest) of a file """
    if os.path.exists(fname):
        hash_md5 = hashlib.md5()
        binary = not istextfile(fname)
        size = os.path.getsize(fname)
        bar = False
        if size >= LARGE_FILE_SIZE:
            bar = True
            msg = "Computing md5 for a large file {}. This is only done once."
            Logger.info(msg.format(os.path.relpath(fname)))
            name = os.path.relpath(fname)
            total = 0

        with open(fname, 'rb') as fobj:
            while True:
                data = fobj.read(LOCAL_CHUNK_SIZE)
                if not data:
                    break

                if bar:
                    total += len(data)
                    progress.update_target(name, total, size)

                if binary:
                    chunk = data
                else:
                    chunk = dos2unix(data)

                hash_md5.update(chunk)

        if bar:
            progress.finish_target(name)

        return (hash_md5.hexdigest(), hash_md5.digest())
    else:
        return (None, None)


def bytes_md5(byts):
    hasher = hashlib.md5()
    hasher.update(byts)
    return hasher.hexdigest()


def dict_filter(d, exclude=[]):
    """
    Exclude specified keys from a nested dict
    """

    if isinstance(d, list):
        ret = []
        for e in d:
            ret.append(dict_filter(e, exclude))
        return ret
    elif isinstance(d, dict):
        ret = {}
        for k, v in d.items():
            assert isinstance(k, str)
            if k in exclude:
                continue
            ret[k] = dict_filter(v, exclude)
        return ret

    return d


def dict_md5(d, exclude=[]):
    filtered = dict_filter(d, exclude)
    byts = json.dumps(filtered, sort_keys=True).encode('utf-8')
    return bytes_md5(byts)


def copyfile(src, dest, no_progress_bar=False, name=None):
    '''Copy file with progress bar'''
    copied = 0
    name = name if name else os.path.basename(dest)
    total = os.stat(src).st_size

    fsrc = open(src, 'rb')

    if os.path.isdir(dest):
        fdest = open(os.path.join(dest, os.path.basename(src)), 'wb+')
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


def move(src, dst):
    dst = os.path.abspath(dst)
    dname = os.path.dirname(dst)
    if not os.path.exists(dname):
        os.makedirs(dname)

    if os.path.islink(src):
        shutil.copy(os.readlink(src), dst)
        os.unlink(src)
        return

    shutil.move(src, dst)


def remove(path):
    if not os.path.exists(path):
        return

    Logger.debug(u'Removing \'{}\''.format(os.path.relpath(path)))

    def _chmod(func, p, excinfo):
        perm = os.stat(p).st_mode
        perm |= stat.S_IWRITE
        os.chmod(p, perm)
        func(p)

    if os.path.isfile(path):
        _chmod(os.unlink, path, None)
    else:
        shutil.rmtree(path, onerror=_chmod)


def to_chunks(l, jobs):
    n = int(math.ceil(len(l) / jobs))

    if len(l) == 1:
        return [l]

    if n == 0:
        n = 1

    return [l[x:x+n] for x in range(0, len(l), n)]


# NOTE: Check if we are in a bundle
# https://pythonhosted.org/PyInstaller/runtime-information.html
def is_binary():
    return getattr(sys, 'frozen', False)


# NOTE: Fix env variables modified by PyInstaller
# http://pyinstaller.readthedocs.io/en/stable/runtime-information.html
def fix_env(env):
    if env is None:
        env = os.environ.copy()
    else:
        env = env.copy()

    if is_binary():
        lp_key = 'LD_LIBRARY_PATH'
        lp_orig = env.get(lp_key + '_ORIG', None)
        if lp_orig is not None:
            env[lp_key] = lp_orig
        else:
            env.pop(lp_key, None)

    return env
