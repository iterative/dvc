"""
Helpers for other modules.
"""
import os
import math
import json
import shutil
import hashlib

from dvc.istextfile import istextfile
from dvc.progress import progress
from dvc.logger import Logger


LOCAL_CHUNK_SIZE = 1024*1024


def dos2unix(data):
    return data.replace(b'\r\n', b'\n')


def file_md5(fname):
    """ get the (md5 hexdigest, md5 digest) of a file """
    if os.path.exists(fname):
        hash_md5 = hashlib.md5()
        binary = not istextfile(fname)

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
    if os.path.isfile(path):
        os.unlink(path)
    else:
        shutil.rmtree(path)


def to_chunks(l, jobs):
    n = int(math.ceil(len(l) / jobs))

    if len(l) == 1:
        return [l]

    if n == 0:
        n = 1

    return [l[x:x+n] for x in range(0, len(l), n)]
