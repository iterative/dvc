import logging
from typing import Optional

from dvc.repo import locked

from .utils import exp_refs, remove_exp_refs

logger = logging.getLogger(__name__)


@locked
def gc(
    repo,
    all_branches: Optional[bool] = False,
    all_tags: Optional[bool] = False,
    all_commits: Optional[bool] = False,
    workspace: Optional[bool] = False,
    commit_date: Optional[str] = None,
    queued: Optional[bool] = False,
):
    keep_revs = set(
        repo.brancher(
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            commit_date=commit_date,
            sha_only=True,
        )
    )
    if workspace:
        keep_revs.add(repo.scm.get_rev())

    if not keep_revs:
        return 0

    to_remove = [
        ref_info
        for ref_info in exp_refs(repo.scm)
        if ref_info.baseline_sha not in keep_revs
    ]
    remove_exp_refs(repo.scm, to_remove)
    removed = len(to_remove)

    delete_stashes = []
    stash = repo.experiments.celery_queue.stash
    for stash_rev, entry in stash.stash_revs.items():
        if not queued or entry.baseline_rev not in keep_revs:
            delete_stashes.append(stash_rev)
    if delete_stashes:
        repo.experiments.celery_queue.remove(delete_stashes)
    removed += len(delete_stashes)

    return removed
