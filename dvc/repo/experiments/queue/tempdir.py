import os
from collections import defaultdict
from collections.abc import Collection, Generator
from typing import TYPE_CHECKING, Optional

from funcy import first

from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.repo.experiments.exceptions import ExpQueueEmptyError
from dvc.repo.experiments.executor.base import ExecutorInfo, TaskStatus
from dvc.repo.experiments.executor.local import TempDirExecutor
from dvc.repo.experiments.utils import EXEC_PID_DIR, EXEC_TMP_DIR
from dvc.utils.objects import cached_property

from .base import BaseStashQueue, QueueEntry, QueueGetResult
from .utils import fetch_running_exp_from_temp_dir
from .workspace import WorkspaceQueue

if TYPE_CHECKING:
    from dvc.repo.experiments import Experiments
    from dvc.repo.experiments.executor.base import BaseExecutor, ExecutorResult
    from dvc.repo.experiments.serialize import ExpRange
    from dvc_task.proc.manager import ProcessManager

logger = logger.getChild(__name__)


_STANDALONE_TMP_DIR = os.path.join(EXEC_TMP_DIR, "standalone")


class TempDirQueue(WorkspaceQueue):
    """Standalone/tempdir exp queue implementation."""

    _EXEC_NAME: Optional[str] = None

    @cached_property
    def _standalone_tmp_dir(self) -> str:
        assert self.repo.tmp_dir is not None
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
            if executor_info.status <= TaskStatus.SUCCESS and os.path.exists(
                executor_info.root_dir
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

    def _reproduce_entry(
        self,
        entry: QueueEntry,
        executor: "BaseExecutor",
        copy_paths: Optional[list[str]] = None,
        message: Optional[str] = None,
        **kwargs,
    ) -> dict[str, dict[str, str]]:
        results: dict[str, dict[str, str]] = defaultdict(dict)
        exec_name = self._EXEC_NAME or entry.stash_rev
        infofile = self.get_infofile_path(exec_name)
        try:
            rev = entry.stash_rev
            exec_result = executor.reproduce(
                info=executor.info,
                rev=rev,
                infofile=infofile,
                log_level=logger.getEffectiveLevel(),
                log_errors=True,
                copy_paths=copy_paths,
                message=message,
            )
            if not exec_result.exp_hash:
                raise DvcException(  # noqa: TRY301
                    f"Failed to reproduce experiment '{rev[:7]}'"
                )
            if exec_result.ref_info:
                results[rev].update(
                    self.collect_executor(self.repo.experiments, executor, exec_result)
                )
        except DvcException:
            raise
        except Exception as exc:
            raise DvcException(f"Failed to reproduce experiment '{rev[:7]}'") from exc
        finally:
            executor.cleanup(infofile)
        return results

    @staticmethod
    def collect_executor(
        exp: "Experiments",
        executor: "BaseExecutor",
        exec_result: "ExecutorResult",
    ) -> dict[str, str]:
        return BaseStashQueue.collect_executor(exp, executor, exec_result)

    def collect_active_data(
        self,
        baseline_revs: Optional[Collection[str]],
        fetch_refs: bool = False,
        **kwargs,
    ) -> dict[str, list["ExpRange"]]:
        from dvc.repo import Repo
        from dvc.repo.experiments.collect import collect_exec_branch
        from dvc.repo.experiments.serialize import (
            ExpExecutor,
            ExpRange,
            LocalExpExecutor,
        )

        result: dict[str, list[ExpRange]] = defaultdict(list)
        for entry in self.iter_active():
            if baseline_revs and entry.baseline_rev not in baseline_revs:
                continue
            if fetch_refs:
                fetch_running_exp_from_temp_dir(self, entry.stash_rev, fetch_refs)
            proc_info = self.proc.get(entry.stash_rev)
            infofile = self.get_infofile_path(entry.stash_rev)
            executor_info = ExecutorInfo.load_json(infofile)
            if proc_info:
                local_exec: Optional[LocalExpExecutor] = LocalExpExecutor(
                    root=executor_info.root_dir,
                    log=proc_info.stdout,
                    pid=proc_info.pid,
                )
            else:
                local_exec = None
            dvc_root = os.path.join(executor_info.root_dir, executor_info.dvc_dir)
            with Repo(dvc_root) as repo:
                exps = list(
                    collect_exec_branch(repo, executor_info.baseline_rev, **kwargs)
                )
            exps[0].rev = entry.stash_rev
            exps[0].name = entry.name
            result[entry.baseline_rev].append(
                ExpRange(
                    exps,
                    executor=ExpExecutor(
                        "running",
                        name=executor_info.location,
                        local=local_exec,
                    ),
                    name=entry.name,
                )
            )
        return result
