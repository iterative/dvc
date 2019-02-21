from __future__ import unicode_literals

import nanotime
import os

import dvc.logger as logger
from dvc.system import System
from dvc.utils.compat import str


def get_inode(path):
    inode = System.inode(path)
    logger.debug("Path {} inode {}".format(path, inode))
    return inode


def get_mtime_and_size(path):
    size = os.path.getsize(path)
    mtime = os.path.getmtime(path)

    if os.path.isdir(path):
        for root, dirs, files in os.walk(str(path)):
            for name in dirs + files:
                entry = os.path.join(root, name)
                stat = os.stat(entry)
                size += stat.st_size
                entry_mtime = stat.st_mtime
                if entry_mtime > mtime:
                    mtime = entry_mtime

    # State of files handled by dvc is stored in db as TEXT.
    # We cast results to string for later comparisons with stored values.
    return str(int(nanotime.timestamp(mtime))), str(size)
