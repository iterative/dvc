import json
import os
from typing import Any, Dict

from celery import chain, shared_task

from dvc.utils.fs import remove

from ..executor.base import EXEC_PID_DIR, EXEC_TMP_DIR
from ..executor.local import TempDirExecutor
from .base import BaseStashQueue, QueueEntry


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
    infofile = os.path.join(repo.tmp_dir, EXEC_TMP_DIR, EXEC_PID_DIR)
    with open(infofile, "w", encoding="utf-8") as fobj:
        json.dump(executor.info.asdict(), fobj)
    cmd = ["dvc", "exp", "exec-run", "--infofile", infofile]

    # schedule execution + cleanup
    chain(
        run(cmd),  # pylint: disable=no-value-for-parameter
        cleanup_exp(  # pylint: disable=no-value-for-parameter
            executor.root_dir, entry.asdict()
        ),
    ).delay()


@shared_task
def cleanup_exp(  # pylint: disable=unused-argument
    proc_dict: Dict[str, Any], entry: Dict[str, Any], tmp_dir: str
) -> None:
    remove(tmp_dir)
