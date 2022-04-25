from functools import wraps
from typing import IO, TYPE_CHECKING, Optional, TypeVar, cast

import fsspec

from dvc.progress import Tqdm

if TYPE_CHECKING:
    from typing import Callable

    from typing_extensions import ParamSpec

    _P = ParamSpec("_P")
    _R = TypeVar("_R")


class FsspecCallback(fsspec.Callback):
    """FsspecCallback usable as a context manager, and a few helper methods."""

    def wrap_attr(self, fobj: IO, method: str = "read") -> IO:
        from tqdm.utils import CallbackIOWrapper

        wrapped = CallbackIOWrapper(self.relative_update, fobj, method)
        return cast(IO, wrapped)

    def wrap_fn(self, fn: "Callable[_P, _R]") -> "Callable[_P, _R]":
        @wraps(fn)
        def wrapped(*args: "_P.args", **kwargs: "_P.kwargs") -> "_R":
            res = fn(*args, **kwargs)
            self.relative_update()
            return res

        return wrapped

    @classmethod
    def as_callback(
        cls, maybe_callback: Optional["FsspecCallback"] = None
    ) -> "FsspecCallback":
        if maybe_callback is None:
            return DEFAULT_CALLBACK
        return maybe_callback


class NoOpCallback(FsspecCallback, fsspec.callbacks.NoOpCallback):
    pass


class TqdmCallback(FsspecCallback):
    def __init__(self, progress_bar):
        self.progress_bar = progress_bar
        super().__init__()

    def set_size(self, size):
        if size is not None:
            self.progress_bar.total = size
            self.progress_bar.refresh()
            super().set_size(size)

    def relative_update(self, inc=1):
        self.progress_bar.update(inc)
        super().relative_update(inc)

    def absolute_update(self, value):
        self.progress_bar.update_to(value)
        super().absolute_update(value)


def tdqm_or_callback_wrapped(
    fobj, method, total, callback=None, **pbar_kwargs
):
    if callback:
        from funcy import nullcontext
        from tqdm.utils import CallbackIOWrapper

        callback.set_size(total)
        wrapper = CallbackIOWrapper(callback.relative_update, fobj, method)
        return nullcontext(wrapper)

    return Tqdm.wrapattr(fobj, method, total=total, bytes=True, **pbar_kwargs)


DEFAULT_CALLBACK = NoOpCallback()
