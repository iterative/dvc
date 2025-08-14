from contextlib import ExitStack
from typing import TYPE_CHECKING, BinaryIO, Optional, Union

from fsspec.callbacks import DEFAULT_CALLBACK, Callback  # noqa: F401
from fsspec.callbacks import TqdmCallback as _TqdmCallback

from dvc.progress import Tqdm
from dvc.utils.objects import cached_property

if TYPE_CHECKING:
    from rich.progress import TaskID
    from tqdm import tqdm

    from dvc.ui._rich_progress import RichTransferProgress


class TqdmCallback(_TqdmCallback):
    def __init__(
        self,
        size: Optional[int] = None,
        value: int = 0,
        progress_bar: Optional["tqdm"] = None,
        tqdm_cls: Optional[type["tqdm"]] = None,
        **tqdm_kwargs,
    ):
        tqdm_kwargs.pop("total", None)
        super().__init__(
            tqdm_kwargs=tqdm_kwargs, tqdm_cls=tqdm_cls or Tqdm, size=size, value=value
        )
        if progress_bar is not None:
            self.tqdm = progress_bar

    def branched(self, path_1: "Union[str, BinaryIO]", path_2: str, **kwargs):
        desc = path_1 if isinstance(path_1, str) else path_2
        return TqdmCallback(bytes=True, desc=desc)


class RichCallback(Callback):
    def __init__(
        self,
        size: Optional[int] = None,
        value: int = 0,
        progress: Optional["RichTransferProgress"] = None,
        desc: Optional[str] = None,
        bytes: bool = False,  # noqa: A002
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

    def branched(self, path_1: Union[str, BinaryIO], path_2: str, **kwargs):
        return RichCallback(
            progress=self.progress,
            desc=path_1 if isinstance(path_1, str) else path_2,
            bytes=True,
            transient=self._transient,
        )
