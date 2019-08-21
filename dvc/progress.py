"""Manages progress bars for dvc repo."""
from __future__ import print_function
import logging
from tqdm import tqdm
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor


class TqdmThreadPoolExecutor(ThreadPoolExecutor):
    """
    Ensure worker progressbars are cleared away properly.
    """

    def __enter__(self):
        """
        Creates a blank initial dummy progress bar if needed so that workers
        are forced to create "nested" bars.
        """
        blank_bar = Tqdm(bar_format="Multi-Threaded:", leave=False)
        if blank_bar.pos > 0:
            # already nested - don't need a placeholder bar
            blank_bar.close()
        self.bar = blank_bar
        return super(TqdmThreadPoolExecutor, self).__enter__()

    def __exit__(self, *a, **k):
        super(TqdmThreadPoolExecutor, self).__exit__(*a, **k)
        self.bar.close()


class Tqdm(tqdm):
    """
    maximum-compatibility tqdm-based progressbars
    """

    def __init__(
        self,
        iterable=None,
        disable=None,
        bytes=False,  # pylint: disable=W0622
        desc_truncate=None,
        leave=None,
        **kwargs
    ):
        """
        bytes   : shortcut for
            `unit='B', unit_scale=True, unit_divisor=1024, miniters=1`
        desc_truncate  : like `desc` but will truncate to 10 chars
        kwargs  : anything accepted by `tqdm.tqdm()`
        """
        kwargs = deepcopy(kwargs)
        if bytes:
            for k, v in dict(
                unit="B", unit_scale=True, unit_divisor=1024, miniters=1
            ).items():
                kwargs.setdefault(k, v)
        if desc_truncate is not None:
            kwargs.setdefault("desc", self.truncate(desc_truncate))
        if disable is None:
            disable = (
                logging.getLogger(__name__).getEffectiveLevel()
                >= logging.CRITICAL
            )
        super(Tqdm, self).__init__(
            iterable=iterable, disable=disable, leave=leave, **kwargs
        )

    def update_desc(self, desc, n=1, truncate=True):
        """
        Calls `set_description(truncate(desc))` and `update(n)`
        """
        self.set_description(
            self.truncate(desc) if truncate else desc, refresh=False
        )
        self.update(n)

    def update_to(self, current, total=None):
        if total:
            self.total = total  # pylint: disable=W0613,W0201
        self.update(current - self.n)

    @classmethod
    def truncate(cls, s, max_len=25, end=True, fill="..."):
        """
        Guarantee len(output) < max_lenself.
        >>> truncate("hello", 4)
        '...o'
        """
        if len(s) <= max_len:
            return s
        if len(fill) > max_len:
            return fill[-max_len:] if end else fill[:max_len]
        i = max_len - len(fill)
        return (fill + s[-i:]) if end else (s[:i] + fill)
