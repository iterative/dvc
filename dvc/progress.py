"""Manages progress bars for dvc repo."""
from __future__ import print_function
import logging
from tqdm import tqdm
from threading import Lock


class Tqdm(tqdm):
    """
    maximum-compatibility tqdm-based progressbars
    """
    def __init__(
            self,
            iterable=None,
            disable=logging.getLogger(
                __name__).getEffectiveLevel() >= logging.CRITICAL,
            ascii=None,  # TODO: True?
            bytes=False,
            **kwargs):
        """
        bytes   : adds unit='B', unit_scale=True, unit_divisor=1024, miniters=1
        kwargs  : anything accepted by `tqdm.tqdm()`
        """
        if bytes:
            #kwargs = deepcopy(kwargs)
            for k, v in dict(unit='B', unit_scale=True, unit_divisor=1024,
                             miniters=1):
                kwargs.setdefault(k, v)
        super(Tqdm, self).__init__(
            iterable=iterable,
            disable=disable,
            ascii=ascii,
            **kwargs)
        # self.set_lock(Lock())

    def update_desc(self, desc, n=1):
        """
        Calls `set_description(desc)` and `update(n)`
        """
        self.set_description(desc, refresh=False)
        self.update(n)


class Progress(tqdm):
    """
    Simple multi-target progress bar.
    TODO: remove this class.
    """
    def __init__(self):
        super(Progress, self).__init__(total=0, disable=True)
        from time import time
        self._time = time
        self.set_lock(Lock())
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
        pass

    def update_target(self, name, current, total, auto_finish=False):
        """Updates progress bar for a specified target."""
        if total and self.total != total:
            self.set_n_total(total)
        self.set_postfix_str(name, refresh=False)
        self.update(current - self.n)
        if auto_finish and self.is_finished:
            self.finish_target(name)

    def finish_target(self, name):
        """Finishes progress bar for a specified target."""
        # TODO: We have to write a msg about finished target
        self.set_postfix_str(name, refresh=False)
        self.clearln()

    def __call__(self, seq, name="", total=None):
        logger = logging.getLogger(__name__)
        logger.warning("DeprecationWarning: create Tqdm() instance instead")
        if total is None:
            total = len(seq)

        self.update_target(name, 0, total)
        for done, item in enumerate(seq, start=1):
            yield item
            self.update_target(name, done, total)
        self.finish_target(name)


class ProgressCallback(object):
    # TODO: remove this thin wrapper
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
