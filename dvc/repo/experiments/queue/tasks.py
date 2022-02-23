import json
import os
from typing import Any, Dict

from celery import chain, shared_task
from celery.utils.log import get_task_logger

from dvc.utils.fs import makedirs, remove

from ..executor.base import EXEC_PID_DIR, EXEC_TMP_DIR, ExecutorInfo
from ..executor.local import TempDirExecutor
from .base import BaseStashQueue, QueueEntry

logger = get_task_logger(__name__)


@shared_task
def setup_exp(entry_dict: Dict[str, Any]) -> None:
    from dvc.repo import Repo
    from dvc_task.proc.tasks import run

    entry = QueueEntry.from_dict(entry_dict)
    repo = Repo(entry.dvc_root)
    # TODO: split executor.init_cache into separate subtask - we can release
    # exp.scm_lock before DVC push
    executor = BaseStashQueue.setup_executor(
        repo.experiments, entry, TempDirExecutor
    )
    pid_dir = os.path.join(
        repo.tmp_dir,
        EXEC_TMP_DIR,
        EXEC_PID_DIR,
    )
    infofile = os.path.join(
        pid_dir,
        f"{entry.stash_rev}{TempDirExecutor.INFOFILE_EXT}",
    )
    makedirs(os.path.dirname(infofile), exist_ok=True)
    with open(infofile, "w", encoding="utf-8") as fobj:
        json.dump(executor.info.asdict(), fobj)
    cmd = ["dvc", "exp", "exec-run", "--infofile", infofile]

    # schedule execution + cleanup
    chain(
        run.si(cmd, wdir=pid_dir, name=entry.stash_rev),
        collect_exp.s(entry.asdict(), infofile),
        cleanup_exp.si(entry.asdict(), executor.root_dir),
    ).delay()


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
    with open(infofile, encoding="utf-8") as fobj:
        executor_info = ExecutorInfo.from_dict(json.load(fobj))
    logger.debug("Collecting experiment info '%s'", str(executor_info))
    executor = TempDirExecutor.from_info(executor_info)
    exec_result = executor_info.result
    if exec_result is not None:
        results = BaseStashQueue.collect_executor(
            repo.experiments, executor, exec_result
        )
        for rev in results:
            logger.debug("Collected experiment '%s'", rev[:7])
    else:
        logger.debug("Exec result was None")


@shared_task(ignore_result=True)
def cleanup_exp(  # pylint: disable=unused-argument
    entry_dict: Dict[str, Any], tmp_dir: str
) -> None:
    remove(tmp_dir)
