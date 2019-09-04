"""Manages dvc lock file."""

from __future__ import unicode_literals

import os
from datetime import timedelta
import hashlib
from flufl.lock import Lock as _Lock, TimeOutError

from dvc.exceptions import DvcException
from dvc.utils import makedirs


class LockError(DvcException):
    """Thrown when unable to acquire the lock for dvc repo."""


class Lock(_Lock):
    """Class for dvc repo lock.

    Args:
        lockfile (str): the lock filename
            in.
        lifetime (int | timedelta): hold the lock for so long.
        tmp_dir (str): a directory to store claim files.
    """

    TIMEOUT = 5

    def __init__(self, lockfile, lifetime=None, tmp_dir=None):
        if isinstance(lifetime, int):
            lifetime = timedelta(seconds=lifetime)

        self._tmp_dir = tmp_dir
        if self._tmp_dir is not None:
            makedirs(self._tmp_dir, exist_ok=True)

        super(Lock, self).__init__(lockfile, lifetime=lifetime)

    @property
    def lockfile(self):
        return self._lockfile

    @property
    def tmp_dir(self):
        return self._tmp_dir

    def lock(self):
        try:
            super(Lock, self).lock(timedelta(seconds=self.TIMEOUT))
        except TimeOutError:
            raise LockError(
                "cannot perform the cmd since DVC is busy and "
                "locked. Please retry the cmd later."
            )

    def _set_claimfile(self, pid=None):
        super(Lock, self)._set_claimfile(pid)

        if self._tmp_dir is not None:
            # Under Windows file path length is limited so we hash it
            filename = hashlib.md5(self._claimfile.encode()).hexdigest()
            self._claimfile = os.path.join(self._tmp_dir, filename + ".lock")
