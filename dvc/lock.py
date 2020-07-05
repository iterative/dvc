"""Manages dvc lock file."""

import hashlib
import os
import time
from datetime import timedelta

import flufl.lock
import zc.lockfile

from dvc.exceptions import DvcException
from dvc.progress import Tqdm
from dvc.utils import format_link

DEFAULT_TIMEOUT = 5

FAILED_TO_LOCK_MESSAGE = (
    "cannot perform the command because another DVC process seems to be "
    "running on this project. If that is not the case, manually remove "
    "`.dvc/tmp/lock` and try again."
)


class LockError(DvcException):
    """Thrown when unable to acquire the lock for DVC repo."""


class Lock:
    """Class for DVC repo lock.

    Uses zc.lockfile as backend.
    """

    def __init__(self, lockfile, friendly=False, **kwargs):
        self._friendly = friendly
        self.lockfile = lockfile
        self._lock = None

    @property
    def files(self):
        return [self.lockfile]

    def _do_lock(self):
        try:
            with Tqdm(
                bar_format="{desc}",
                disable=not self._friendly,
                desc=(
                    "If DVC froze, see `hardlink_lock` in {}".format(
                        format_link("https://man.dvc.org/config#core")
                    )
                ),
            ):
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

    @property
    def is_locked(self):
        return bool(self._lock)

    def __enter__(self):
        self.lock()

    def __exit__(self, typ, value, tbck):
        self.unlock()


class HardlinkLock(flufl.lock.Lock):
    """Class for DVC repo lock.

    Args:
        lockfile (str): the lock filename
            in.
        tmp_dir (str): a directory to store claim files.
    """

    def __init__(
        self, lockfile, tmp_dir=None, **kwargs
    ):  # pylint: disable=super-init-not-called
        import socket

        self._tmp_dir = tmp_dir

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

    def lock(self):  # pylint: disable=arguments-differ
        try:
            super().lock(timedelta(seconds=DEFAULT_TIMEOUT))
        except flufl.lock.TimeOutError:
            raise LockError(FAILED_TO_LOCK_MESSAGE)

    def _set_claimfile(self, pid=None):
        super()._set_claimfile(pid)

        if self._tmp_dir is not None:
            # Under Windows file path length is limited so we hash it
            filename = hashlib.md5(self._claimfile.encode()).hexdigest()
            self._claimfile = os.path.join(self._tmp_dir, filename + ".lock")

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


def make_lock(lockfile, tmp_dir=None, friendly=False, hardlink_lock=False):
    cls = HardlinkLock if hardlink_lock else Lock
    return cls(lockfile, tmp_dir=tmp_dir, friendly=friendly)
