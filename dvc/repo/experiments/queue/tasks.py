import json
import os
from typing import TYPE_CHECKING, Any, Dict

from celery import chain, shared_task

from dvc.utils.fs import remove

from .. import scm_locked
from ..executor.base import EXEC_PID_DIR, EXEC_TMP_DIR
from ..executor.local import TempDirExecutor
from ..refs import EXEC_BASELINE, EXEC_HEAD, EXEC_MERGE
from ..stash import ExpStash, ExpStashEntry
from .base import QueueEntry

if TYPE_CHECKING:
    from dvc.repo.experiments import Experiments


@shared_task
def setup_exp(entry_dict: Dict[str, Any]) -> None:
    from dvc.repo import Repo
    from dvc_task.proc.tasks import run

    entry = QueueEntry.from_dict(entry_dict)
    repo = Repo(entry.dvc_root)
    executor = _setup_executor(repo.experiments, entry)
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


@scm_locked
def _setup_executor(
    exp: "Experiments", queue_entry: QueueEntry
) -> TempDirExecutor:
    scm = exp.scm
    stash = ExpStash(scm, queue_entry.stash_ref)
    stash_rev = queue_entry.stash_rev
    stash_entry = stash.stash_revs.get(
        stash_rev,
        ExpStashEntry(None, stash_rev, stash_rev, None, None),
    )
    if stash_entry.stash_index is not None:
        stash.drop(stash_entry.index)

    scm.set_ref(EXEC_HEAD, stash_entry.head_rev)
    scm.set_ref(EXEC_MERGE, stash_rev)
    scm.set_ref(EXEC_BASELINE, stash_entry.baseline_rev)

    # Executor will be initialized with an empty git repo that
    # we populate by pushing:
    #   EXEC_HEAD - the base commit for this experiment
    #   EXEC_MERGE - the unmerged changes (from our stash)
    #       to be reproduced
    #   EXEC_BASELINE - the baseline commit for this experiment

    # TODO: split executor.init_cache into separate subtask - we can release
    # exp.scm_lock before DVC push
    return TempDirExecutor.from_stash_entry(exp.repo, stash_rev, stash_entry)


@shared_task
def cleanup_exp(  # pylint: disable=unused-argument
    proc_dict: Dict[str, Any], entry: Dict[str, Any], tmp_dir: str
) -> None:
    remove(tmp_dir)
