"""Manages progress bars for dvc repo."""

from __future__ import print_function
from __future__ import unicode_literals

from dvc.utils.compat import str

import sys
import threading
import dvc.logger as logger


class Progress(object):
    """
    Simple multi-target progress bar.
    """

    def __init__(self):
        self._n_total = 0
        self._n_finished = 0
        self._lock = threading.Lock()
        self._line = None

    def set_n_total(self, total):
        """Sets total number of targets."""
        self._n_total = total
        self._n_finished = 0

    @property
    def is_finished(self):
        """Returns if all targets have finished."""
        return self._n_total == self._n_finished

    def _clearln(self):
        self._print("\r\x1b[K", end="")

    def _writeln(self, line):
        self._clearln()
        self._print(line, end="")
        sys.stdout.flush()

    def refresh(self, line=None):
        """Refreshes progress bar."""
        # Just go away if it is locked. Will update next time
        if not self._lock.acquire(False):
            return

        if line is None:
            line = self._line

        if sys.stdout.isatty() and line is not None:
            self._writeln(line)
            self._line = line

        self._lock.release()

    def update_target(self, name, current, total):
        """Updates progress bar for a specified target."""
        self.refresh(self._bar(name, current, total))

    def finish_target(self, name):
        """Finishes progress bar for a specified target."""
        # We have to write a msg about finished target
        with self._lock:
            pbar = self._bar(name, 100, 100)

            if sys.stdout.isatty():
                self._clearln()

            self._print(pbar)

            self._n_finished += 1
            self._line = None

    def _bar(self, target_name, current, total):
        """
        Make a progress bar out of info, which looks like:
        (1/2): [########################################] 100% master.zip
        """
        bar_len = 30

        if total is None:
            state = 0
            percent = "?% "
        else:
            total = int(total)
            state = int((100 * current) / total) if current < total else 100
            percent = str(state) + "% "

        if self._n_total > 1:
            num = "({}/{}): ".format(self._n_finished + 1, self._n_total)
        else:
            num = ""

        n_sh = int((state * bar_len) / 100)
        n_sp = bar_len - n_sh
        pbar = "[" + "#" * n_sh + " " * n_sp + "] "

        return num + pbar + percent + target_name

    @staticmethod
    def _print(*args, **kwargs):
        if logger.is_quiet():
            return

        print(*args, **kwargs)

    def __enter__(self):
        self._lock.acquire(True)
        if self._line is not None:
            self._clearln()

    def __exit__(self, typ, value, tbck):
        if self._line is not None:
            self.refresh()
        self._lock.release()


progress = Progress()  # pylint: disable=invalid-name
