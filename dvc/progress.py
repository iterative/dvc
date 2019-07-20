"""Manages progress bars for dvc repo."""

from __future__ import print_function
from __future__ import unicode_literals
import logging
from tqdm import tqdm

from dvc.utils.compat import str

import sys
import threading


class Progress(tqdm):
    """
    Simple multi-target progress bar.
    """
    def __init__(self):
        super(Progress, self).__init__(
            total=0,
            disable=logging.getLogger(__name__).getEffectiveLevel() >= logging.CRITICAL)
        self.set_lock(threading.Lock())
        self._targets = {}
        self.clearln()

    def set_n_total(self, total):
        """Sets total number of targets."""
        self.reset(total)

    @property
    def is_finished(self):
        """Returns if all targets have finished."""
        return self.total == self.n

    def clearln(self):
        self.display("")

    def update_target(self, name, current, total, auto_finish=False):
        """Updates progress bar for a specified target."""
        if name in self._targets:
            t = self._targets[name]
        else:
            with self.get_lock():
                # TODO: up to 10 nested bars
                position = 1 + self._get_free_pos(self) % 10
            t = tqdm(
                total=total,
                desc=name,
                leave=False,
                position=position)
            self._targets[name] = t
            self.total += 1

        t.update(current - t.n)
        if auto_finish and t.n == t.total:
            self.finish_target(name)

    def finish_target(self, name):
        """Finishes progress bar for a specified target."""
        t = self._targets.pop(name)
        t.close()
        # TODO: We have to write a msg about finished target
        if self.total < 100:
            # only if less that 100 items
            print(t)
        self.update()

    def __call__(self, seq, name="", total=None):
        if total is None:
            total = len(seq)

        self.update_target(name, 0, total)
        for done, item in enumerate(seq, start=1):
            yield item
            self.update_target(name, done, total)
        self.finish_target(name)


class ProgressCallback(object):
    # TODO: is this meant to be a thin wrapper for multi-progress, or just one bar?
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
