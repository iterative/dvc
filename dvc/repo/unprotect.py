from __future__ import unicode_literals

import os
import stat
import uuid

import dvc.logger as logger
from dvc.system import System
from dvc.utils import copyfile, move, remove
from dvc.exceptions import DvcException


def _unprotect_file(path):
    if System.is_symlink(path) or System.is_hardlink(path):
        logger.debug("Unprotecting '{}'".format(path))

        tmp = os.path.join(os.path.dirname(path), "." + str(uuid.uuid4()))
        move(path, tmp)

        copyfile(tmp, path)

        remove(tmp)
    else:
        logger.debug(
            "Skipping copying for '{}', since it is not "
            "a symlink or a hardlink.".format(path)
        )

    os.chmod(path, os.stat(path).st_mode | stat.S_IWRITE)


def _unprotect_dir(path):
    for root, dirs, files in os.walk(str(path)):
        for f in files:
            path = os.path.join(root, f)
            _unprotect_file(path)


def unprotect(path):
    if not os.path.exists(path):
        raise DvcException(
            "can't unprotect non-existing data '{}'".format(path)
        )

    if os.path.isdir(path):
        _unprotect_dir(path)
    else:
        _unprotect_file(path)
