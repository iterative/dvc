import json
import os
from collections import defaultdict
from collections.abc import Collection, Generator
from typing import TYPE_CHECKING, Optional

import psutil
from funcy import first

from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.repo.experiments.exceptions import ExpQueueEmptyError
from dvc.repo.experiments.executor.base import ExecutorInfo, TaskStatus
from dvc.repo.experiments.executor.local import WorkspaceExecutor
from dvc.repo.experiments.refs import EXEC_BRANCH, WORKSPACE_STASH
from dvc.repo.experiments.utils import get_exp_rwlock
from dvc.utils.fs import remove
from dvc.utils.serialize import load_json

from .base import BaseStashQueue, QueueEntry, QueueGetResult

if TYPE_CHECKING:
    from dvc.repo.experiments import Experiments
    from dvc.repo.experiments.executor.base import BaseExecutor, ExecutorResult
    from dvc.repo.experiments.serialize import ExpRange

    from .base import QueueDoneResult

logger = logger.getChild(__name__)


class WorkspaceQueue(BaseStashQueue):
    _EXEC_NAME: Optional[str] = "workspace"

    def put(self, *args, **kwargs) -> QueueEntry:
        kwargs.pop("copy_paths", None)
        with get_exp_rwlock(self.repo, writes=["workspace", WORKSPACE_STASH]):
            return self._stash_exp(*args, **kwargs)

    def get(self) -> QueueGetResult:
        revs = self.stash.stash_revs
        if not revs:
            raise ExpQueueEmptyError("No experiments in the queue.")
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
        executor = self.init_executor(self.repo.experiments, entry)
        return QueueGetResult(entry, executor)

    def iter_queued(self) -> Generator[QueueEntry, None, None]:
        for rev, entry in self.stash.stash_revs.items():
            yield QueueEntry(
                self.repo.root_dir,
                self.scm.root_dir,
                self.ref,
                rev,
                entry.baseline_rev,
                entry.branch,
                entry.name,
                entry.head_rev,
            )

    def iter_active(self) -> Generator[QueueEntry, None, None]:
        # Workspace run state is reflected in the workspace itself and does not
        # need to be handled via the queue
        raise NotImplementedError

    def iter_done(self) -> Generator["QueueDoneResult", None, None]:
        raise NotImplementedError

    def iter_failed(self) -> Generator["QueueDoneResult", None, None]:
        raise NotImplementedError

    def iter_success(self) -> Generator["QueueDoneResult", None, None]:
        raise NotImplementedError

    def reproduce(
        self, copy_paths: Optional[list[str]] = None, message: Optional[str] = None
    ) -> dict[str, dict[str, str]]:
        results: dict[str, dict[str, str]] = defaultdict(dict)
        try:
            while True:
                entry, executor = self.get()
                results.update(
                    self._reproduce_entry(
                        entry, executor, copy_paths=copy_paths, message=message
                    )
                )
        except ExpQueueEmptyError:
            pass
        return results

    def _reproduce_entry(
        self, entry: QueueEntry, executor: "BaseExecutor", **kwargs
    ) -> dict[str, dict[str, str]]:
        kwargs.pop("copy_paths", None)
        from dvc_task.proc.process import ProcessInfo

        results: dict[str, dict[str, str]] = defaultdict(dict)
        exec_name = self._EXEC_NAME or entry.stash_rev
        proc_info = ProcessInfo(os.getpid(), None, None, None, None)
        proc_info_path = self._proc_info_path(exec_name)
        os.makedirs(os.path.dirname(proc_info_path), exist_ok=True)
        proc_info.dump(proc_info_path)
        infofile = self.get_infofile_path(exec_name)
        try:
            rev = entry.stash_rev
            exec_result = executor.reproduce(
                info=executor.info,
                rev=rev,
                infofile=infofile,
                log_level=logger.getEffectiveLevel(),
                log_errors=not isinstance(executor, WorkspaceExecutor),
                message=kwargs.get("message"),
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
            remove(self._proc_info_path(exec_name))
        return results

    def _proc_info_path(self, name: str) -> str:
        return os.path.join(self.pid_dir, name, f"{name}.json")

    @property
    def _active_pid(self) -> Optional[int]:
        from dvc_task.proc.process import ProcessInfo

        assert self._EXEC_NAME
        name = self._EXEC_NAME
        try:
            proc_info = ProcessInfo.load(self._proc_info_path(name))
            pid = proc_info.pid
            if psutil.pid_exists(pid):
                return pid
            logger.debug("Workspace exec PID '%d' no longer exists, removing.", pid)
            remove(self._proc_info_path(name))
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        return None

    @staticmethod
    def collect_executor(
        exp: "Experiments",
        executor: "BaseExecutor",  # noqa: ARG004
        exec_result: "ExecutorResult",
    ) -> dict[str, str]:
        results: dict[str, str] = {}
        exp_rev = exp.scm.get_ref(EXEC_BRANCH)
        if exp_rev:
            assert exec_result.exp_hash
            logger.debug("Collected experiment '%s'.", exp_rev[:7])
            results[exp_rev] = exec_result.exp_hash

        return results

    def get_result(self, entry: QueueEntry) -> Optional["ExecutorResult"]:
        raise NotImplementedError

    def kill(self, revs: Collection[str]) -> None:
        raise NotImplementedError

    def shutdown(self, kill: bool = False):
        raise NotImplementedError

    def logs(self, rev: str, encoding: Optional[str] = None, follow: bool = False):
        raise NotImplementedError

    def get_running_exp(self) -> Optional[str]:
        """Return the name of the exp running in workspace (if it exists)."""
        assert self._EXEC_NAME
        if self._active_pid is None:
            return None

        infofile = self.get_infofile_path(self._EXEC_NAME)
        try:
            info = ExecutorInfo.from_dict(load_json(infofile))
        except OSError:
            return None
        return info.name

    def collect_active_data(
        self,
        baseline_revs: Optional[Collection[str]],
        fetch_refs: bool = False,  # noqa: ARG002
        **kwargs,
    ) -> dict[str, list["ExpRange"]]:
        from dvc.repo.experiments.collect import collect_exec_branch
        from dvc.repo.experiments.serialize import (
            ExpExecutor,
            ExpRange,
            LocalExpExecutor,
        )

        result: dict[str, list[ExpRange]] = defaultdict(list)
        pid = self._active_pid
        if pid is None:
            return result

        assert self._EXEC_NAME
        infofile = self.get_infofile_path(self._EXEC_NAME)
        try:
            info = ExecutorInfo.from_dict(load_json(infofile))
        except OSError:
            return result

        if (
            (not baseline_revs or info.baseline_rev in baseline_revs)
            and info.status < TaskStatus.FAILED
            and info.status != TaskStatus.SUCCESS
        ):
            local_exec = LocalExpExecutor(root=info.root_dir, pid=pid)
            exps = list(collect_exec_branch(self.repo, info.baseline_rev, **kwargs))
            exps[0].name = info.name
            result[info.baseline_rev] = [
                ExpRange(
                    exps,
                    executor=ExpExecutor("running", name="workspace", local=local_exec),
                    name=info.name,
                )
            ]
        return result

    def collect_queued_data(
        self,
        baseline_revs: Optional[Collection[str]],
        **kwargs,
    ) -> dict[str, list["ExpRange"]]:
        raise NotImplementedError

    def collect_failed_data(
        self,
        baseline_revs: Optional[Collection[str]],
        **kwargs,
    ) -> dict[str, list["ExpRange"]]:
        raise NotImplementedError
