from typing import Any, Dict, List

from celery import shared_task
from celery.signals import task_postrun
from celery.utils.log import get_task_logger

from dvc.utils.fs import remove

from ..executor.base import ExecutorInfo
from ..executor.local import TempDirExecutor
from .base import BaseStashQueue, QueueEntry

logger = get_task_logger(__name__)


@shared_task
def setup_exp(entry_dict: Dict[str, Any]) -> None:
    """Setup an experiment.

    Arguments:
        entry_dict: Serialized QueueEntry for this experiment.
    """
    from dvc.repo import Repo

    entry = QueueEntry.from_dict(entry_dict)
    repo = Repo(entry.dvc_root)
    # TODO: split executor.init_cache into separate subtask - we can release
    # exp.scm_lock before DVC push
    executor = BaseStashQueue.setup_executor(
        repo.experiments, entry, TempDirExecutor
    )
    infofile = repo.experiments.celery_queue.get_infofile_path(entry.stash_rev)
    executor.info.dump_json(infofile)


@shared_task
def collect_exp(
    proc_dict: Dict[str, Any],
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
    from dvc_task.proc.process import ProcessInfo

    proc_info = ProcessInfo.from_dict(proc_dict)
    if proc_info.returncode != 0:
        # TODO: handle errors, track failed exps separately
        pass

    entry = QueueEntry.from_dict(entry_dict)
    repo = Repo(entry.dvc_root)
    infofile = repo.experiments.celery_queue.get_infofile_path(entry.stash_rev)
    executor_info = ExecutorInfo.load_json(infofile)
    logger.debug("Collecting experiment info '%s'", str(executor_info))
    executor = TempDirExecutor.from_info(executor_info)
    exec_result = executor_info.result
    try:
        if exec_result is not None:
            results = BaseStashQueue.collect_executor(
                repo.experiments, executor, exec_result
            )
            for rev in results:
                logger.debug("Collected experiment '%s'", rev[:7])
        else:
            logger.debug("Exec result was None")
    except Exception:  # pylint: disable=broad-except
        # Log exceptions but do not re-raise so that task chain execution
        # continues
        logger.exception("Failed to collect experiment")
    return executor.root_dir


@shared_task
def cleanup_exp(  # pylint: disable=unused-argument
    tmp_dir: str, entry_dict: Dict[str, Any]
) -> None:
    """Cleanup after an experiment.

    Arguments:
        tmp_dir: Temp directory to be removed.
        entry_dict: Serialized QueueEntry for this experiment.
    """
    remove(tmp_dir)


@task_postrun.connect(sender=cleanup_exp)
def _cleanup_postrun_handler(
    args: List[Any] = None,
    **kwargs,
):
    from dvc.repo import Repo

    assert args
    (_, entry_dict) = args
    entry = QueueEntry.from_dict(entry_dict)
    repo = Repo(entry.dvc_root)
    infofile = repo.experiments.celery_queue.get_infofile_path(entry.stash_rev)
    executor_info = ExecutorInfo.load_json(infofile)
    executor_info.collected = True
    executor_info.dump_json(infofile)
