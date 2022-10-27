import logging
import os
from typing import TYPE_CHECKING, Dict, Generator, Optional

from funcy import cached_property, first

from ..exceptions import ExpQueueEmptyError
from ..executor.base import (
    EXEC_PID_DIR,
    EXEC_TMP_DIR,
    BaseExecutor,
    ExecutorInfo,
    ExecutorResult,
    TaskStatus,
)
from ..executor.local import TempDirExecutor
from .base import BaseStashQueue, QueueEntry, QueueGetResult
from .utils import fetch_running_exp_from_temp_dir
from .workspace import WorkspaceQueue

if TYPE_CHECKING:
    from dvc.repo.experiments import Experiments
    from dvc_task.proc.manager import ProcessManager

logger = logging.getLogger(__name__)


_STANDALONE_TMP_DIR = os.path.join(EXEC_TMP_DIR, "standalone")


class TempDirQueue(WorkspaceQueue):
    """Standalone/tempdir exp queue implementation."""

    _EXEC_NAME: Optional[str] = None

    @cached_property
    def _standalone_tmp_dir(self) -> str:
        return os.path.join(self.repo.tmp_dir, _STANDALONE_TMP_DIR)

    @cached_property
    def pid_dir(self) -> str:
        return os.path.join(self._standalone_tmp_dir, EXEC_PID_DIR)

    @cached_property
    def proc(self) -> "ProcessManager":
        from dvc_task.proc.manager import ProcessManager

        return ProcessManager(self.pid_dir)

    def get(self) -> QueueGetResult:
        revs = self.stash.stash_revs
        if not revs:
            raise ExpQueueEmptyError("No stashed standalone experiments.")
        stash_rev, stash_entry = first(revs.items())
        entry = QueueEntry(
            self.repo.root_dir,
            self.scm.root_dir,
            self.ref,
            stash_rev,
            stash_entry.baseline_rev,
            stash_entry.branch,
            stash_entry.name,
            stash_entry.head_rev,
        )
        executor = self.init_executor(
            self.repo.experiments,
            entry,
            TempDirExecutor,
            wdir=self._standalone_tmp_dir,
        )
        return QueueGetResult(entry, executor)

    def iter_active(self) -> Generator[QueueEntry, None, None]:
        # NOTE: Yielded queue entries are not complete for performance reasons.
        # Retrieving exec ref information is unavailable without doing a
        # git-fetch, and is unneeded in the common use cases for iter_active.
        for stash_rev in self.proc:
            infofile = self.get_infofile_path(stash_rev)
            executor_info = ExecutorInfo.load_json(infofile)
            if (
                not executor_info.status <= TaskStatus.SUCCESS
                and os.path.exists(executor_info.root_dir)
            ):
                yield QueueEntry(
                    self.repo.root_dir,
                    self.scm.root_dir,
                    self.ref,
                    stash_rev,
                    executor_info.baseline_rev,
                    None,  # branch unavailable without doing a git-fetch
                    executor_info.name,
                    None,
                )

    @staticmethod
    def collect_executor(
        exp: "Experiments",
        executor: BaseExecutor,
        exec_result: ExecutorResult,
    ) -> Dict[str, str]:
        return BaseStashQueue.collect_executor(exp, executor, exec_result)

    def get_running_exps(self, fetch_refs: bool = True) -> Dict[str, Dict]:
        result: Dict[str, Dict] = {}
        for entry in self.iter_active():
            result.update(
                fetch_running_exp_from_temp_dir(
                    self, entry.stash_rev, fetch_refs
                )
            )
        return result
