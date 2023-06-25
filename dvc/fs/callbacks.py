# pylint: disable=unused-import
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, BinaryIO, Dict, Optional, Union

from dvc.progress import Tqdm
from dvc.utils.objects import cached_property
from dvc_objects.fs.callbacks import DEFAULT_CALLBACK, Callback  # noqa: F401

if TYPE_CHECKING:
    from rich.progress import TaskID

    from dvc.ui._rich_progress import RichTransferProgress


# NOTE: this is very similar to dvc-objects.fs.callbacks.TqdmCallback,
# but it works with our own Tqdm instance.
class TqdmCallback(Callback):
    def __init__(
        self,
        size: Optional[int] = None,
        value: int = 0,
        progress_bar: Optional["Tqdm"] = None,
        **tqdm_kwargs,
    ):
        tqdm_kwargs["total"] = size or -1
        self._tqdm_kwargs = tqdm_kwargs
        self._progress_bar = progress_bar
        self._stack = ExitStack()
        super().__init__(size=size, value=value)

    @cached_property
    def progress_bar(self):
        progress_bar = (
            self._progress_bar
            if self._progress_bar is not None
            else Tqdm(**self._tqdm_kwargs)
        )
        return self._stack.enter_context(progress_bar)

    def __enter__(self):
        return self

    def close(self):
        self._stack.close()

    def set_size(self, size):
        # Tqdm tries to be smart when to refresh,
        # so we try to force it to re-render.
        super().set_size(size)
        self.progress_bar.refresh()

    def call(self, hook_name=None, **kwargs):  # noqa: ARG002
        self.progress_bar.update_to(self.value, total=self.size)

    def branch(
        self,
        path_1: "Union[str, BinaryIO]",
        path_2: str,
        kwargs: Dict[str, Any],
        child: Optional[Callback] = None,
    ):
        child = child or TqdmCallback(
            bytes=True, desc=path_1 if isinstance(path_1, str) else path_2
        )
        return super().branch(path_1, path_2, kwargs, child=child)


class RichCallback(Callback):
    def __init__(
        self,
        size: Optional[int] = None,
        value: int = 0,
        progress: Optional["RichTransferProgress"] = None,
        desc: Optional[str] = None,
        bytes: bool = False,  # noqa: A002, pylint: disable=redefined-builtin
        unit: Optional[str] = None,
        disable: bool = False,
        transient: bool = True,
    ) -> None:
        self._progress = progress
        self.disable = disable
        self._task_kwargs = {
            "description": desc or "",
            "bytes": bytes,
            "unit": unit,
            "total": size or 0,
            "visible": False,
            "progress_type": None if bytes else "summary",
        }
        self._transient = transient
        self._stack = ExitStack()
        super().__init__(size=size, value=value)

    @cached_property
    def progress(self) -> "RichTransferProgress":
        from dvc.ui import ui
        from dvc.ui._rich_progress import RichTransferProgress

        if self._progress is not None:
            return self._progress

        progress = RichTransferProgress(
            transient=self._transient,
            disable=self.disable,
            console=ui.error_console,
        )
        self._stack.enter_context(progress)
        return progress

    @cached_property
    def task(self) -> "TaskID":
        return self.progress.add_task(**self._task_kwargs)  # type: ignore[arg-type]

    def __enter__(self):
        return self

    def close(self):
        if self._transient:
            self.progress.clear_task(self.task)
        self._stack.close()

    def call(self, hook_name=None, **kwargs):  # noqa: ARG002
        self.progress.update(
            self.task,
            completed=self.value,
            total=self.size,
            visible=not self.disable,
        )

    def branch(
        self,
        path_1: Union[str, BinaryIO],
        path_2: str,
        kwargs: Dict[str, Any],
        child: Optional["Callback"] = None,
    ):
        child = child or RichCallback(
            progress=self.progress,
            desc=path_1 if isinstance(path_1, str) else path_2,
            bytes=True,
            transient=self._transient,
        )
        return super().branch(path_1, path_2, kwargs, child=child)
