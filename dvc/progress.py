"""Manages progress bars for dvc repo."""

from __future__ import print_function
from __future__ import unicode_literals

from dvc.utils.compat import str

import sys
import threading

CLEARLINE_PATTERN = "\r\x1b[K"


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

    def clearln(self):
        self.print(CLEARLINE_PATTERN, end="")

    def _writeln(self, line):
        self.clearln()
        self.print(line, end="")
        sys.stdout.flush()

    def reset(self):
        with self._lock:
            self._n_total = 0
            self._n_finished = 0
            self._line = None

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
                self.clearln()

            self.print(pbar)

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
    def print(*args, **kwargs):
        import dvc.logger as logger

        if logger.is_quiet():
            return

        print(*args, **kwargs)

    def __enter__(self):
        self._lock.acquire(True)
        if self._line is not None:
            self.clearln()

    def __exit__(self, typ, value, tbck):
        if self._line is not None:
            self.refresh()
        self._lock.release()


def progress_aware(f):
    """ Decorator to add a new line if progress bar hasn't finished  """
    from functools import wraps

    @wraps(f)
    def wrapper(*args, **kwargs):
        if not progress.is_finished:
            progress.print()
        progress.clearln()

        return f(*args, **kwargs)

    return wrapper


class ProgressCallback(object):
    def __init__(self, total):
        self.total = total
        self.current = 0
        progress.reset()

    def update(self, name, progress_to_add=1):
        self.current += progress_to_add
        progress.update_target(name, self.current, self.total)

    def finish(self, name):
        progress.finish_target(name)


progress = Progress()  # pylint: disable=invalid-name
