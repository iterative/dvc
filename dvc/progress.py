"""Manages progress bars for dvc repo."""
from __future__ import print_function
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

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
        "{percentage:3.0f}%|{bar:10}|{desc} {bar:-10b}{n}/{total}"
        " [{elapsed}<{remaining}, {rate_fmt:>11}{postfix}]"
    )
    BAR_FMT_NOTOTAL = (
        "{desc} {bar:b}{n} [{elapsed}<??:??, {rate_fmt:>11}{postfix}]"
    )

    def __init__(
        self,
        iterable=None,
        disable=None,
        level=logging.ERROR,
        desc=None,
        desc_truncate=None,
        leave=None,
        level_leave=logging.DEBUG,
        bar_format=None,
        bytes=False,  # pylint: disable=W0622
        **kwargs
    ):
        """
        bytes   : shortcut for
            `unit='B', unit_scale=True, unit_divisor=1024, miniters=1`
        desc  : persists after `close()`
        desc_truncate  : like `desc` but will `truncate()` and not persist
        level  : effective logging level for determining `disable`;
            used only if `disable` is unspecified
        level_leave  : effective logging level for determining `leave`;
            used only if `leave` is unspecified
        kwargs  : anything accepted by `tqdm.tqdm()`
        """
        kwargs = kwargs.copy()
        kwargs.setdefault("unit_scale", True)
        if bytes:
            for k, v in dict(
                unit="B", unit_scale=True, unit_divisor=1024, miniters=1
            ).items():
                kwargs.setdefault(k, v)
        self.desc_persist = desc
        if desc_truncate is not None:
            desc = self.truncate(desc_truncate)
        if disable is None:
            disable = logger.getEffectiveLevel() > level
        if leave is None:
            leave = logger.getEffectiveLevel() <= level_leave
        if bar_format is None:
            if kwargs.get("total", hasattr(iterable, "__len__")):
                bar_format = self.BAR_FMT_DEFAULT
            else:
                bar_format = self.BAR_FMT_NOTOTAL
        super(Tqdm, self).__init__(
            iterable=iterable,
            disable=disable,
            leave=leave,
            desc=desc,
            bar_format=bar_format,
            **kwargs
        )

    def update_desc(self, desc, n=1, truncate=True):
        """
        Calls `set_description_str(truncate(desc))` and `update(n)`
        """
        self.set_description_str(
            self.truncate(desc) if truncate else desc, refresh=False
        )
        self.update(n)

    def update_to(self, current, total=None):
        if total:
            self.total = total  # pylint: disable=W0613,W0201
        self.update(current - self.n)

    def close(self):
        if self.desc_persist:
            self.set_description_str(self.desc_persist, refresh=False)
        super(Tqdm, self).close()

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
