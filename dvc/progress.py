"""Manages progress bars for DVC repo."""

import logging
import sys
from threading import RLock

from tqdm import tqdm

from dvc.utils import env2bool

logger = logging.getLogger(__name__)
tqdm.set_lock(RLock())


class Tqdm(tqdm):
    """
    maximum-compatibility tqdm-based progressbars
    """

    BAR_FMT_DEFAULT = (
        "{percentage:3.0f}% {desc}|{bar}|"
        "{n_fmt}/{total_fmt}"
        " [{elapsed}<{remaining}, {rate_fmt:>11}{postfix}]"
    )
    # nested bars should have fixed bar widths to align nicely
    BAR_FMT_DEFAULT_NESTED = (
        "{percentage:3.0f}%|{bar:10}|{desc:{ncols_desc}.{ncols_desc}}"
        "{n_fmt}/{total_fmt}"
        " [{elapsed}<{remaining}, {rate_fmt:>11}{postfix}]"
    )
    BAR_FMT_NOTOTAL = (
        "{desc:{ncols_desc}.{ncols_desc}}{n_fmt}"
        " [{elapsed}, {rate_fmt:>11}{postfix}]"
    )
    BYTES_DEFAULTS = dict(
        unit="B", unit_scale=True, unit_divisor=1024, miniters=1
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
        file=None,
        total=None,
        **kwargs
    ):
        """
        bytes   : shortcut for
            `unit='B', unit_scale=True, unit_divisor=1024, miniters=1`
        desc  : persists after `close()`
        level  : effective logging level for determining `disable`;
            used only if `disable` is unspecified
        disable  : If (default: None), will be determined by logging level.
            May be overridden to `True` due to non-TTY status.
            Skip override by specifying env var `DVC_IGNORE_ISATTY`.
        kwargs  : anything accepted by `tqdm.tqdm()`
        """
        kwargs = kwargs.copy()
        if bytes:
            kwargs = {**self.BYTES_DEFAULTS, **kwargs}
        else:
            kwargs.setdefault("unit_scale", total > 999 if total else True)
        if file is None:
            file = sys.stderr
        self.desc_persist = desc
        # auto-disable based on `logger.level`
        if disable is None:
            disable = logger.getEffectiveLevel() > level
        # auto-disable based on TTY
        if (
            not disable
            and not env2bool("DVC_IGNORE_ISATTY")
            and hasattr(file, "isatty")
        ):
            disable = not file.isatty()
        super().__init__(
            iterable=iterable,
            disable=disable,
            leave=leave,
            desc=desc,
            bar_format="!",
            lock_args=(False,),
            total=total,
            **kwargs
        )
        if bar_format is None:
            if self.__len__():
                self.bar_format = (
                    self.BAR_FMT_DEFAULT_NESTED
                    if self.pos
                    else self.BAR_FMT_DEFAULT
                )
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
        # unknown/zero ETA
        self.bar_format = self.bar_format.replace("<{remaining}", "")
        # remove completed bar
        self.bar_format = self.bar_format.replace("|{bar:10}|", " ")
        super().close()

    @property
    def format_dict(self):
        """inject `ncols_desc` to fill the display width (`ncols`)"""
        d = super().format_dict
        ncols = d["ncols"] or 80
        ncols_desc = ncols - len(self.format_meter(ncols_desc=1, **d)) + 1
        ncols_desc = max(ncols_desc, 0)
        if ncols_desc:
            d["ncols_desc"] = ncols_desc
        else:
            # work-around for zero-width description
            d["ncols_desc"] = 1
            d["prefix"] = ""
        return d
