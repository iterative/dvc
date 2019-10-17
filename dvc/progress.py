"""Manages progress bars for dvc repo."""
from __future__ import print_function
import logging
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from funcy import merge

logger = logging.getLogger(__name__)


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

    BAR_FMT_DEFAULT = (
        "{percentage:3.0f}%|{bar:10}|"
        "{desc:{ncols_desc}.{ncols_desc}}{n}/{total}"
        " [{elapsed}<{remaining}, {rate_fmt:>11}{postfix}]"
    )
    BAR_FMT_NOTOTAL = (
        "{desc:{ncols_desc}.{ncols_desc}}{n}"
        " [{elapsed}<??:??, {rate_fmt:>11}{postfix}]"
    )

    def __init__(
        self,
        iterable=None,
        disable=None,
        level=logging.ERROR,
        desc=None,
        leave=False,
        bar_format=None,
        bytes=False,  # pylint: disable=W0622
        **kwargs
    ):
        """
        bytes   : shortcut for
            `unit='B', unit_scale=True, unit_divisor=1024, miniters=1`
        desc  : persists after `close()`
        level  : effective logging level for determining `disable`;
            used only if `disable` is unspecified
        kwargs  : anything accepted by `tqdm.tqdm()`
        """
        kwargs = kwargs.copy()
        kwargs.setdefault("unit_scale", True)
        if bytes:
            bytes_defaults = dict(
                unit="B", unit_scale=True, unit_divisor=1024, miniters=1
            )
            kwargs = merge(bytes_defaults, kwargs)
        self.desc_persist = desc
        if disable is None:
            disable = logger.getEffectiveLevel() > level
        super(Tqdm, self).__init__(
            iterable=iterable,
            disable=disable,
            leave=leave,
            desc=desc,
            bar_format="!",
            **kwargs
        )
        if bar_format is None:
            if self.__len__():
                self.bar_format = self.BAR_FMT_DEFAULT
            else:
                self.bar_format = self.BAR_FMT_NOTOTAL
        else:
            self.bar_format = bar_format
        self.refresh()

    def update_desc(self, desc, n=1):
        """
        Calls `set_description_str(desc)` and `update(n)`
        """
        self.set_description_str(desc, refresh=False)
        self.update(n)

    def update_to(self, current, total=None):
        if total:
            self.total = total  # pylint: disable=W0613,W0201
        self.update(current - self.n)

    def close(self):
        if self.desc_persist is not None:
            self.set_description_str(self.desc_persist, refresh=False)
        super(Tqdm, self).close()

    @property
    def format_dict(self):
        """inject `ncols_desc` to fill the display width (`ncols`)"""
        d = super(Tqdm, self).format_dict
        ncols = d["ncols"] or 80
        ncols_desc = ncols - len(self.format_meter(ncols_desc=1, **d)) + 1
        d["ncols_desc"] = max(ncols_desc, 0)
        return d
