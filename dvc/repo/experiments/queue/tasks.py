from typing import TYPE_CHECKING, Any, Dict, List, Optional

from celery import shared_task
from celery.utils.log import get_task_logger

from dvc.repo.experiments.executor.base import ExecutorInfo
from dvc.repo.experiments.executor.local import TempDirExecutor

from .base import BaseStashQueue, QueueEntry

if TYPE_CHECKING:
    from dvc.repo.experiments.executor.base import BaseExecutor


logger = get_task_logger(__name__)


@shared_task
def setup_exp(entry_dict: Dict[str, Any]) -> "BaseExecutor":
    """Setup an experiment.

    Arguments:
        entry_dict: Serialized QueueEntry for this experiment.

    Returns:
        Root executor (temp) directory for this experiment.
    """
    from dvc.repo import Repo

    entry = QueueEntry.from_dict(entry_dict)
    with Repo(entry.dvc_root) as repo:
        # TODO: split executor.init_cache into separate subtask - we can release
        # exp.scm_lock before DVC push
        executor = BaseStashQueue.init_executor(
            repo.experiments,
            entry,
            TempDirExecutor,
            location="dvc-task",
        )
        infofile = repo.experiments.celery_queue.get_infofile_path(entry.stash_rev)
        executor.info.dump_json(infofile)
    return executor


@shared_task
def collect_exp(
    proc_dict: Dict[str, Any],  # noqa: ARG001
    entry_dict: Dict[str, Any],
) -> str:
    """Collect results for an experiment.

    Arguments:
        proc_dict: Serialized ProcessInfo for experiment executor process.
        entry_dict: Serialized QueueEntry for this experiment.

    Returns:
        Directory to be cleaned up after this experiment.
    """
    from dvc.repo import Repo

    entry = QueueEntry.from_dict(entry_dict)
    with Repo(entry.dvc_root) as repo:
        celery_queue = repo.experiments.celery_queue
        infofile = celery_queue.get_infofile_path(entry.stash_rev)
        executor_info = ExecutorInfo.load_json(infofile)
        logger.debug("Collecting experiment info '%s'", str(executor_info))
        executor = TempDirExecutor.from_info(executor_info)
        exec_result = executor_info.result
        try:
            if exec_result is not None:
                BaseStashQueue.collect_executor(repo.experiments, executor, exec_result)
            else:
                logger.debug("Experiment failed (Exec result was None)")
                celery_queue.stash_failed(entry)
        except Exception:
            # Log exceptions but do not re-raise so that task chain execution
            # continues
            logger.exception("Failed to collect experiment")
    return executor.root_dir


@shared_task
def cleanup_exp(executor: TempDirExecutor, infofile: str) -> None:
    """Cleanup after an experiment.

    Arguments:
        tmp_dir: Temp directory to be removed.
        entry_dict: Serialized QueueEntry for this experiment.
    """
    executor.cleanup(infofile)


@shared_task
def run_exp(
    entry_dict: Dict[str, Any],
    copy_paths: Optional[List[str]] = None,
    message: Optional[str] = None,
) -> None:
    """Run a full experiment.

    Experiment subtasks are executed inline as one atomic operation.

    Arguments:
        entry_dict: Serialized QueueEntry for this experiment.
    """
    from dvc.repo import Repo

    entry = QueueEntry.from_dict(entry_dict)
    with Repo(entry.dvc_root) as repo:
        queue = repo.experiments.celery_queue
        infofile = queue.get_infofile_path(entry.stash_rev)
    executor = setup_exp.s(entry_dict)()
    try:
        cmd = ["dvc", "exp", "exec-run", "--infofile", infofile]
        if copy_paths:
            for path in copy_paths:
                cmd.extend(["--copy-paths", path])
        if message:
            cmd.extend(["--message", message])
        proc_dict = queue.proc.run_signature(cmd, name=entry.stash_rev)()
        collect_exp.s(proc_dict, entry_dict)()
    finally:
        cleanup_exp.s(executor, infofile)()
