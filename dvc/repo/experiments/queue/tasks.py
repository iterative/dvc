from typing import Any, Dict

from celery import chain, shared_task
from celery.utils.log import get_task_logger

from dvc.utils.fs import remove

from ..executor.base import ExecutorInfo
from ..executor.local import TempDirExecutor
from .base import BaseStashQueue, QueueEntry

logger = get_task_logger(__name__)


@shared_task
def setup_exp(entry_dict: Dict[str, Any]) -> str:
    """Setup (queue) an experiment.

    Arguments:
        entry_dict: Serialized QueueEntry for this experiment.

    Returns:
        Celery task ID for the queued experiment task chain.
    """
    from dvc.repo import Repo
    from dvc_task.proc.tasks import run

    entry = QueueEntry.from_dict(entry_dict)
    repo = Repo(entry.dvc_root)
    # TODO: split executor.init_cache into separate subtask - we can release
    # exp.scm_lock before DVC push
    executor = BaseStashQueue.setup_executor(
        repo.experiments, entry, TempDirExecutor
    )
    proc = repo.experiments.celery_queue.proc
    infofile = repo.experiments.celery_queue.get_infofile_path(entry.stash_rev)
    executor.info.dump_json(infofile)
    cmd = ["dvc", "exp", "exec-run", "--infofile", infofile]

    # schedule execution + cleanup
    exp_chain = chain(
        proc.run(cmd, name=entry.stash_rev),
        collect_exp.s(entry.asdict(), infofile),
        cleanup_exp.si(entry.asdict(), executor.root_dir),
    )
    exp_chain.freeze()
    exp_chain.delay()
    return exp_chain.id


@shared_task
def collect_exp(
    proc_dict: Dict[str, Any],
    entry_dict: Dict[str, Any],
    infofile: str,
) -> None:
    from dvc.repo import Repo
    from dvc_task.proc.process import ProcessInfo

    proc_info = ProcessInfo.from_dict(proc_dict)
    if proc_info.returncode != 0:
        # TODO: handle errors, track failed exps separately
        pass

    entry = QueueEntry.from_dict(entry_dict)
    repo = Repo(entry.dvc_root)
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
    finally:
        executor_info.collected = True
        executor_info.dump_json(infofile)


@shared_task(ignore_result=True)
def cleanup_exp(  # pylint: disable=unused-argument
    entry_dict: Dict[str, Any], tmp_dir: str
) -> None:
    remove(tmp_dir)
