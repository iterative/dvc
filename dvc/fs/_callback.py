from contextlib import ExitStack
from functools import wraps
from typing import IO, TYPE_CHECKING, Any, Dict, Optional, TypeVar, cast

import fsspec
from funcy import cached_property

if TYPE_CHECKING:
    from typing import Callable

    from typing_extensions import ParamSpec

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

    def wrap_and_branch(self, fn: "Callable") -> "Callable":
        """
        Wraps a function, and pass a new child callback to it.
        When the function completes, we increment the parent callback by 1.
        """
        wrapped = self.wrap_fn(fn)

        @wraps(fn)
        def func(path1: str, path2: str):
            kw: Dict[str, Any] = {}
            with self.branch(path1, path2, kw):
                return wrapped(path1, path2, **kw)

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

    def branch(
        self,
        path_1: str,
        path_2: str,
        kwargs: Dict[str, Any],
        child: "FsspecCallback" = None,
    ) -> "FsspecCallback":
        child = kwargs["callback"] = child or DEFAULT_CALLBACK
        return child


class NoOpCallback(FsspecCallback, fsspec.callbacks.NoOpCallback):
    pass


class TqdmCallback(FsspecCallback):
    def __init__(self, progress_bar: "Tqdm" = None, **tqdm_kwargs):
        self._tqdm_kwargs = tqdm_kwargs
        self._progress_bar = progress_bar
        self._stack = ExitStack()
        super().__init__()

    @cached_property
    def progress_bar(self):
        from dvc.progress import Tqdm

        return self._stack.enter_context(
            self._progress_bar
            if self._progress_bar is not None
            else Tqdm(**self._tqdm_kwargs)
        )

    def __enter__(self):
        return self

    def close(self):
        self._stack.close()

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

    def branch(
        self,
        path_1: str,
        path_2: str,
        kwargs,
        child: Optional[FsspecCallback] = None,
    ):
        child = child or TqdmCallback(bytes=True, total=-1, desc=path_1)
        return super().branch(path_1, path_2, kwargs, child=child)


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

    def branch(
        self, path_1, path_2, kwargs, child: Optional[FsspecCallback] = None
    ):
        child = child or RichCallback(
            self.progress, desc=path_1, bytes=True, total=-1
        )
        return super().branch(path_1, path_2, kwargs, child=child)


DEFAULT_CALLBACK = NoOpCallback()
