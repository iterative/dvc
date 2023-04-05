import json
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Collection, Dict, Generator, List, Optional

import psutil
from funcy import first
from voluptuous import Invalid

from dvc.exceptions import DvcException
from dvc.lock import make_lock
from dvc.repo.experiments.exceptions import ExpQueueEmptyError
from dvc.repo.experiments.executor.base import ExecutorInfo, TaskStatus
from dvc.repo.experiments.executor.local import WorkspaceExecutor
from dvc.repo.experiments.refs import EXEC_BRANCH, WORKSPACE_STASH
from dvc.repo.experiments.utils import get_exp_rwlock
from dvc.rwlock import RWLOCK_FILE, RWLOCK_LOCK, SCHEMA
from dvc.utils import relpath
from dvc.utils.fs import remove

from .base import BaseStashQueue, QueueEntry, QueueGetResult

if TYPE_CHECKING:
    from dvc.repo.experiments import Experiments
    from dvc.repo.experiments.executor.base import BaseExecutor, ExecutorResult

    from .base import QueueDoneResult

logger = logging.getLogger(__name__)


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
        self, copy_paths: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, str]]:
        results: Dict[str, Dict[str, str]] = defaultdict(dict)
        try:
            while True:
                entry, executor = self.get()
                results.update(
                    self._reproduce_entry(entry, executor, copy_paths=copy_paths)
                )
        except ExpQueueEmptyError:
            pass
        return results

    def _reproduce_entry(
        self, entry: QueueEntry, executor: "BaseExecutor", **kwargs
    ) -> Dict[str, Dict[str, str]]:
        kwargs.pop("copy_paths", None)
        from dvc.stage.monitor import CheckpointKilledError

        results: Dict[str, Dict[str, str]] = defaultdict(dict)
        exec_name = self._EXEC_NAME or entry.stash_rev
        infofile = self.get_infofile_path(exec_name)
        try:
            rev = entry.stash_rev
            exec_result = executor.reproduce(
                info=executor.info,
                rev=rev,
                infofile=infofile,
                log_level=logger.getEffectiveLevel(),
                log_errors=not isinstance(executor, WorkspaceExecutor),
            )
            if not exec_result.exp_hash:
                raise DvcException(f"Failed to reproduce experiment '{rev[:7]}'")
            if exec_result.ref_info:
                results[rev].update(
                    self.collect_executor(self.repo.experiments, executor, exec_result)
                )
        except CheckpointKilledError:
            # Checkpoint errors have already been logged
            return {}
        except DvcException:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DvcException(f"Failed to reproduce experiment '{rev[:7]}'") from exc
        finally:
            executor.cleanup(infofile)
        return results

    @staticmethod
    def collect_executor(  # pylint: disable=unused-argument
        exp: "Experiments",
        executor: "BaseExecutor",  # noqa: ARG004
        exec_result: "ExecutorResult",
    ) -> Dict[str, str]:
        results: Dict[str, str] = {}
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

    def logs(
        self,
        rev: str,
        encoding: Optional[str] = None,
        follow: bool = False,
    ):
        raise NotImplementedError

    def check_rwlock(
        self,
        hardlink: bool = False,
        autocorrect: bool = False,
    ) -> bool:
        """Check and autocorrect the RWLock status for file paths.

        Args:
            hardlink (bool): use hardlink lock to guard rwlock file when on
                            edit.
            autocorrect (bool): autocorrect corrupted rwlock file.

        Return:
            (bool): if the pid alive.
        """
        path = self.repo.fs.path.join(self.repo.tmp_dir, RWLOCK_FILE)

        rwlock_guard = make_lock(
            self.repo.fs.path.join(self.repo.tmp_dir, RWLOCK_LOCK),
            tmp_dir=self.repo.tmp_dir,
            hardlink_lock=hardlink,
        )
        with rwlock_guard:
            try:
                with self.repo.fs.open(path, encoding="utf-8") as fobj:
                    lock: Dict[str, List[Dict]] = SCHEMA(json.load(fobj))
                file_path = first(lock["read"])
                if not file_path:
                    return False
                lock_info = first(lock["read"][file_path])
                pid = int(lock_info["pid"])
                if psutil.pid_exists(pid):
                    return True
                cmd = lock_info["cmd"]
                logger.warning(
                    "Process '%s' with (Pid %s), in RWLock-file '%s' had been killed.",
                    cmd,
                    pid,
                    relpath(path),
                )
            except FileNotFoundError:
                return False
            except json.JSONDecodeError:
                logger.warning(
                    "Unable to read RWLock-file '%s'. JSON structure is corrupted",
                    relpath(path),
                )
            except Invalid:
                logger.warning("RWLock-file '%s' format error.", relpath(path))
            if autocorrect:
                logger.warning("Delete corrupted RWLock-file '%s'", relpath(path))
                remove(path)
            return False

    def get_running_exps(
        self,
        fetch_refs: bool = True,  # noqa: ARG002
    ) -> Dict[str, Dict]:
        from dvc.utils.serialize import load_json

        assert self._EXEC_NAME
        result: Dict[str, Dict] = {}

        if not self.check_rwlock(autocorrect=True):
            return result

        infofile = self.get_infofile_path(self._EXEC_NAME)

        try:
            info = ExecutorInfo.from_dict(load_json(infofile))
        except OSError:
            return result

        if info.status < TaskStatus.FAILED:
            # If we are appending to a checkpoint branch in a workspace
            # run, show the latest checkpoint as running.
            if info.status == TaskStatus.SUCCESS:
                return result
            last_rev = self.scm.get_ref(EXEC_BRANCH)
            if last_rev:
                result[last_rev] = info.asdict()
            else:
                result[self._EXEC_NAME] = info.asdict()
        return result
