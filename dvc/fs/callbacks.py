# pylint: disable=unused-import
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, BinaryIO, Dict, Optional, Union

from funcy import cached_property

from dvc_objects.fs.callbacks import (  # noqa: F401
    DEFAULT_CALLBACK,
    Callback,
    TqdmCallback,
)

if TYPE_CHECKING:
    from dvc.ui._rich_progress import RichTransferProgress


class RichCallback(Callback):
    def __init__(
        self,
        size: Optional[int] = None,
        value: int = 0,
        progress: "RichTransferProgress" = None,
        desc: str = None,
        bytes: bool = False,  # pylint: disable=redefined-builtin
        unit: str = None,
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
    def progress(self):
        from dvc.ui import ui
        from dvc.ui._rich_progress import RichTransferProgress

        if self._progress is not None:
            return self._progress

        progress = RichTransferProgress(
            transient=self._transient,
            disable=self.disable,
            console=ui.error_console,
        )
        return self._stack.enter_context(progress)

    @cached_property
    def task(self):
        return self.progress.add_task(**self._task_kwargs)

    def __enter__(self):
        return self

    def close(self):
        if self._transient:
            self.progress.clear_task(self.task)
        self._stack.close()

    def call(self, hook_name=None, **kwargs):
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
        child: "Callback" = None,
    ):
        child = child or RichCallback(
            progress=self.progress,
            desc=path_1 if isinstance(path_1, str) else path_2,
            bytes=True,
            transient=self._transient,
        )
        return super().branch(path_1, path_2, kwargs, child=child)
