import logging
import os
from abc import ABC
from collections import defaultdict, deque
from typing import TYPE_CHECKING, Deque, Dict, Optional, Tuple, Type

from dvc.proc.manager import ProcessManager

from ..base import (
    EXEC_BASELINE,
    EXEC_BRANCH,
    EXEC_HEAD,
    EXEC_MERGE,
    CheckpointExistsError,
    ExperimentExistsError,
    ExpRefInfo,
    ExpStashEntry,
)
from .base import EXEC_PID_DIR, BaseExecutor
from .local import TempDirExecutor, WorkspaceExecutor

if TYPE_CHECKING:
    from scmrepo.git import Git

    from dvc.repo import Repo

logger = logging.getLogger(__name__)


class BaseExecutorManager(ABC):
    """Manages executors for a collection of experiments to be run."""

    EXECUTOR_CLS: Type = BaseExecutor

    def __init__(
        self,
        scm: "Git",
        wdir: str,
    ):
        from dvc.utils.fs import makedirs

        self.scm = scm
        makedirs(wdir, exist_ok=True)
        self.wdir = wdir
        self.executors = self._load_infos()
        self._queue: Deque[Tuple[str, "BaseExecutor"]] = deque()
        self.proc = ProcessManager(self.pid_dir)

    @property
    def pid_dir(self) -> str:
        return os.path.join(self.wdir, EXEC_PID_DIR)

    def enqueue(self, rev: str, executor: "BaseExecutor"):
        self._queue.append((rev, executor))

    def _load_infos(self) -> Dict[str, "BaseExecutor"]:
        # TODO: pending process manager changes
        # import json
        # from urllib.parse import urlparse
        # from .base import ExecutorInfo
        # from .ssh import SSHExecutor

        # def make_executor(info: "ExecutorInfo"):
        #     if info.git_url:
        #         scheme = urlparse(info.git_url)
        #         if scheme == "file":
        #             cls = TempDirExecutor
        #         elif scheme == "ssh":
        #             cls = SSHExecutor
        #         else:
        #             raise NotImplementedError
        #     else:
        #         cls = WorkspaceExecutor
        #     return cls.from_info(info)

        infos: Dict[str, "BaseExecutor"] = {}
        # for name in self.proc.processes():
        #     infofile = os.path.join(
        #         name, f"{name}{BaseExecutor.INFOFILE_EXT}"
        #     )
        #     try:
        #         with open(infofile, encoding="utf-8") as fobj:
        #             info = ExecutorInfo.from_dict(json.load(fobj))
        #         infos[name] = make_executor(info)
        #     except OSError:
        #         continue
        return infos

    @classmethod
    def from_stash_entries(
        cls,
        scm: "Git",
        wdir: str,
        repo: "Repo",
        to_run: Dict[str, ExpStashEntry],
        **kwargs,
    ):
        manager = cls(scm, wdir)
        try:
            for stash_rev, entry in to_run.items():
                scm.set_ref(EXEC_HEAD, entry.head_rev)
                scm.set_ref(EXEC_MERGE, stash_rev)
                scm.set_ref(EXEC_BASELINE, entry.baseline_rev)

                # Executor will be initialized with an empty git repo that
                # we populate by pushing:
                #   EXEC_HEAD - the base commit for this experiment
                #   EXEC_MERGE - the unmerged changes (from our stash)
                #       to be reproduced
                #   EXEC_BASELINE - the baseline commit for this experiment
                executor = cls.EXECUTOR_CLS.from_stash_entry(
                    repo,
                    stash_rev,
                    entry,
                    **kwargs,
                )
                manager.enqueue(stash_rev, executor)
        finally:
            for ref in (EXEC_HEAD, EXEC_MERGE, EXEC_BASELINE):
                scm.remove_ref(ref)
        return manager

    def exec_queue(self, jobs: Optional[int] = 1):
        """Run dvc repro for queued executors in parallel."""
        import signal
        from concurrent.futures import (
            CancelledError,
            ProcessPoolExecutor,
            wait,
        )
        from multiprocessing import Manager

        from dvc.stage.monitor import CheckpointKilledError

        result: Dict[str, Dict[str, str]] = defaultdict(dict)

        manager = Manager()
        pid_q = manager.Queue()

        with ProcessPoolExecutor(max_workers=jobs) as workers:
            futures = {}
            while self._queue:
                rev, executor = self._queue.popleft()
                infofile = os.path.join(
                    self.pid_dir,
                    f"{rev}{executor.INFOFILE_EXT}",
                )
                future = workers.submit(
                    executor.reproduce,
                    info=executor.info,
                    rev=rev,
                    queue=pid_q,
                    infofile=infofile,
                    log_level=logger.getEffectiveLevel(),
                )
                futures[future] = (rev, executor)

            try:
                wait(futures)
            except KeyboardInterrupt:
                # forward SIGINT to any running executor processes and
                # cancel any remaining futures
                workers.shutdown(wait=False)
                pids = {}
                for future, (rev, _) in futures.items():
                    if future.running():
                        # if future has already been started by the scheduler
                        # we still have to wait until it tells us its PID
                        while rev not in pids:
                            rev, pid = pid_q.get()
                            pids[rev] = pid
                        os.kill(pids[rev], signal.SIGINT)
                    elif not future.done():
                        future.cancel()

            for future, (rev, executor) in futures.items():
                rev, executor = futures[future]

                try:
                    exc = future.exception()
                    if exc is None:
                        exec_result = future.result()
                        result[rev].update(
                            self._collect_executor(executor, exec_result)
                        )
                    elif not isinstance(exc, CheckpointKilledError):
                        logger.error(
                            "Failed to reproduce experiment '%s'", rev[:7]
                        )
                except CancelledError:
                    logger.error(
                        "Cancelled before attempting to reproduce experiment "
                        "'%s'",
                        rev[:7],
                    )
                finally:
                    executor.cleanup()

        return result

    def _collect_executor(self, executor, exec_result) -> Dict[str, str]:
        # NOTE: GitPython Repo instances cannot be re-used
        # after process has received SIGINT or SIGTERM, so we
        # need this hack to re-instantiate git instances after
        # checkpoint runs. See:
        # https://github.com/gitpython-developers/GitPython/issues/427
        # del self.repo.scm

        results = {}

        def on_diverged(ref: str, checkpoint: bool):
            ref_info = ExpRefInfo.from_ref(ref)
            if checkpoint:
                raise CheckpointExistsError(ref_info.name)
            raise ExperimentExistsError(ref_info.name)

        for ref in executor.fetch_exps(
            self.scm,
            executor.git_url,
            force=exec_result.force,
            on_diverged=on_diverged,
        ):
            exp_rev = self.scm.get_ref(ref)
            if exp_rev:
                logger.debug("Collected experiment '%s'.", exp_rev[:7])
                results[exp_rev] = exec_result.exp_hash

        return results


class TempDirExecutorManager(BaseExecutorManager):
    EXECUTOR_CLS = TempDirExecutor


class WorkspaceExecutorManager(BaseExecutorManager):
    EXECUTOR_CLS = WorkspaceExecutor

    @classmethod
    def from_stash_entries(
        cls,
        scm: "Git",
        wdir: str,
        repo: "Repo",
        to_run: Dict[str, ExpStashEntry],
        **kwargs,
    ):
        manager = cls(scm, wdir)
        try:
            assert len(to_run) == 1
            for stash_rev, entry in to_run.items():
                scm.set_ref(EXEC_HEAD, entry.head_rev)
                scm.set_ref(EXEC_MERGE, stash_rev)
                scm.set_ref(EXEC_BASELINE, entry.baseline_rev)

                executor = cls.EXECUTOR_CLS.from_stash_entry(
                    repo,
                    stash_rev,
                    entry,
                    **kwargs,
                )
                manager.enqueue(stash_rev, executor)
        finally:
            for ref in (EXEC_MERGE,):
                scm.remove_ref(ref)
        return manager

    def _collect_executor(self, executor, exec_result) -> Dict[str, str]:
        results = {}
        exp_rev = self.scm.get_ref(EXEC_BRANCH)
        if exp_rev:
            logger.debug("Collected experiment '%s'.", exp_rev[:7])
            results[exp_rev] = exec_result.exp_hash
        return results

    def exec_queue(self, jobs: Optional[int] = 1):
        """Run a single WorkspaceExecutor.

        Workspace execution is done within the main DVC process
        (rather than in multiprocessing context)
        """
        from dvc.exceptions import DvcException
        from dvc.stage.monitor import CheckpointKilledError

        assert len(self._queue) == 1
        result: Dict[str, Dict[str, str]] = defaultdict(dict)
        rev, executor = self._queue.popleft()
        infofile = os.path.join(
            self.pid_dir,
            f"{rev}{executor.INFOFILE_EXT}",
        )
        try:
            exec_result = executor.reproduce(
                info=executor.info,
                rev=rev,
                infofile=infofile,
                log_level=logger.getEffectiveLevel(),
            )
            if not exec_result.exp_hash:
                raise DvcException(
                    f"Failed to reproduce experiment '{rev[:7]}'"
                )
            if exec_result.ref_info:
                result[rev].update(
                    self._collect_executor(executor, exec_result)
                )
        except CheckpointKilledError:
            # Checkpoint errors have already been logged
            return {}
        except DvcException:
            raise
        except Exception as exc:
            raise DvcException(
                f"Failed to reproduce experiment '{rev[:7]}'"
            ) from exc
        finally:
            executor.cleanup()
        return result
