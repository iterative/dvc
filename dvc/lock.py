"""Manages dvc lock file."""
from __future__ import unicode_literals

import hashlib
import os
import time
from datetime import timedelta

from funcy.py3 import lkeep

from dvc.exceptions import DvcException
from dvc.utils import makedirs
from dvc.utils.compat import is_py3


DEFAULT_TIMEOUT = 5

FAILED_TO_LOCK_MESSAGE = (
    "cannot perform the command because another DVC process seems to be "
    "running on this project. If that is not the case, manually remove "
    "`.dvc/lock` and try again."
)


class LockError(DvcException):
    """Thrown when unable to acquire the lock for dvc repo."""


if is_py3:
    import flufl.lock

    class Lock(flufl.lock.Lock):
        """Class for dvc repo lock.

        Args:
            lockfile (str): the lock filename
                in.
            tmp_dir (str): a directory to store claim files.
        """

        def __init__(self, lockfile, tmp_dir=None):
            import socket

            self._tmp_dir = tmp_dir
            if self._tmp_dir is not None:
                makedirs(self._tmp_dir, exist_ok=True)

            # NOTE: this is basically Lock.__init__ copy-paste, except that
            # instead of using `socket.getfqdn()` we use `socket.gethostname()`
            # to speed this up. We've seen [1] `getfqdn()` take ~5sec to return
            # anything, which is way too slow. `gethostname()` is actually a
            # fallback for `getfqdn()` when it is not able to resolve a
            # canonical hostname through network. The claimfile that uses
            # `self._hostname` is still usable, as it uses `pid` and random
            # number to generate the resulting lock file name, which is unique
            # enough for our application.
            #
            # [1] https://github.com/iterative/dvc/issues/2582
            self._hostname = socket.gethostname()

            self._lockfile = lockfile
            self._lifetime = timedelta(days=365)  # Lock for good by default
            self._separator = flufl.lock.SEP
            self._set_claimfile()
            self._owned = True
            self._retry_errnos = []

        @property
        def lockfile(self):
            return self._lockfile

        @property
        def files(self):
            return lkeep([self._lockfile, self._tmp_dir])

        def lock(self):
            try:
                super(Lock, self).lock(timedelta(seconds=DEFAULT_TIMEOUT))
            except flufl.lock.TimeOutError:
                raise LockError(FAILED_TO_LOCK_MESSAGE)

        def _set_claimfile(self, pid=None):
            super(Lock, self)._set_claimfile(pid)

            if self._tmp_dir is not None:
                # Under Windows file path length is limited so we hash it
                filename = hashlib.md5(self._claimfile.encode()).hexdigest()
                self._claimfile = os.path.join(
                    self._tmp_dir, filename + ".lock"
                )

        # Fix for __del__ bug in flufl.lock [1] which is causing errors on
        # Python shutdown [2].
        # [1] https://gitlab.com/warsaw/flufl.lock/issues/7
        # [2] https://github.com/iterative/dvc/issues/2573
        def __del__(self):
            try:
                if self._owned:
                    self.finalize()
            except ImportError:
                pass


else:
    import zc.lockfile

    class Lock(object):
        """Class for dvc repo lock.

        Uses zc.lockfile as backend.
        """

        def __init__(self, lockfile, tmp_dir=None):
            self.lockfile = lockfile
            self._lock = None

        @property
        def files(self):
            return [self.lockfile]

        def _do_lock(self):
            try:
                self._lock = zc.lockfile.LockFile(self.lockfile)
            except zc.lockfile.LockError:
                raise LockError(FAILED_TO_LOCK_MESSAGE)

        def lock(self):
            try:
                self._do_lock()
                return
            except LockError:
                time.sleep(DEFAULT_TIMEOUT)

            self._do_lock()

        def unlock(self):
            self._lock.close()
            self._lock = None

        def __enter__(self):
            self.lock()

        def __exit__(self, typ, value, tbck):
            self.unlock()
