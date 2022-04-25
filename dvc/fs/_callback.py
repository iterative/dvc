from functools import wraps
from typing import IO, TYPE_CHECKING, Any, Optional, TypeVar, cast

import fsspec

if TYPE_CHECKING:
    from typing import Callable

    from typing_extensions import ParamSpec

    from dvc.fs.base import FileSystem
    from dvc.progress import Tqdm
    from dvc.ui._rich_progress import RichTransferProgress

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

    def wrap_and_branch(
        self, fn: "Callable", fs: "FileSystem" = None
    ) -> "Callable":
        """
        Wraps a function, and pass a new child callback to it.
        When the function completes, we increment the parent callback by 1.
        """
        from .local import localfs

        fs = fs or localfs
        wrapped = self.wrap_fn(fn)

        def make_callback(path1, path2):
            # pylint: disable=assignment-from-none
            child = self.branch(fs.path.name(path1), path2, {})
            return self.as_callback(child)

        @wraps(fn)
        def func(path1, path2, **kwargs):
            with make_callback(path1, path2) as callback:
                return wrapped(path1, path2, callback=callback, **kwargs)

        return func

    def __enter__(self):
        return self

    def __exit__(self, *exc_args):
        self.close()

    def close(self):
        """Handle here on exit."""

    @classmethod
    def as_callback(
        cls, maybe_callback: Optional["FsspecCallback"] = None
    ) -> "FsspecCallback":
        if maybe_callback is None:
            return DEFAULT_CALLBACK
        return maybe_callback

    @classmethod
    def as_tqdm_callback(
        cls,
        callback: Optional["FsspecCallback"] = None,
        **tqdm_kwargs: Any,
    ) -> "FsspecCallback":
        return callback or TqdmCallback(**tqdm_kwargs)

    @classmethod
    def as_rich_callback(
        cls, callback: Optional["FsspecCallback"] = None, **rich_kwargs
    ):
        return callback or RichCallback(**rich_kwargs)


class NoOpCallback(FsspecCallback, fsspec.callbacks.NoOpCallback):
    pass


class TqdmCallback(FsspecCallback):
    def __init__(self, progress_bar: "Tqdm" = None, **tqdm_kwargs):
        from dvc.progress import Tqdm

        self.progress_bar = progress_bar or Tqdm(**tqdm_kwargs)
        super().__init__()

    def __enter__(self):
        self.progress_bar.__enter__()
        return self

    def close(self):
        self.progress_bar.close()

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

    def branch(self, path_1: str, path_2: str, kwargs):
        return TqdmCallback(bytes=True, total=-1, desc=path_1, **kwargs)


class RichCallback(FsspecCallback):
    def __init__(
        self,
        progress: "RichTransferProgress" = None,
        desc: str = None,
        total: int = None,
        bytes: bool = False,  # pylint: disable=redefined-builtin
        unit: str = None,
        disable: bool = False,
    ) -> None:
        from dvc.ui import ui
        from dvc.ui._rich_progress import RichTransferProgress

        self.progress = progress or RichTransferProgress(
            transient=True,
            disable=disable,
            console=ui.error_console,
        )
        self.visible = not disable
        self._newly_created = progress is None
        total = 0 if total is None or total < 0 else total
        self.task = self.progress.add_task(
            description=desc or "",
            total=total,
            bytes=bytes,
            visible=False,
            unit=unit,
            progress_type=None if bytes else "summary",
        )
        super().__init__()

    def __enter__(self):
        if self._newly_created:
            self.progress.__enter__()
        return self

    def close(self):
        if self._newly_created:
            self.progress.stop()
        try:
            self.progress.remove_task(self.task)
        except KeyError:
            pass

    def set_size(self, size: int = None) -> None:
        if size is not None:
            self.progress.update(self.task, total=size, visible=self.visible)
            super().set_size(size)

    def relative_update(self, inc: int = 1) -> None:
        self.progress.update(self.task, advance=inc)
        super().relative_update(inc)

    def absolute_update(self, value: int) -> None:
        self.progress.update(self.task, completed=value)
        super().absolute_update(value)

    def branch(self, path_1, path_2, kwargs):
        return RichCallback(self.progress, desc=path_1, bytes=True, total=-1)


DEFAULT_CALLBACK = NoOpCallback()
