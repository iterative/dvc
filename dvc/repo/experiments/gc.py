import logging
from typing import Optional

from dvc.repo import locked

logger = logging.getLogger(__name__)


@locked
def gc(
    repo,
    all_branches: Optional[bool] = False,
    all_tags: Optional[bool] = False,
    all_commits: Optional[bool] = False,
    workspace: Optional[bool] = False,
    queued: Optional[bool] = False,
):
    keep_revs = set(
        repo.brancher(
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
            sha_only=True,
        )
    )
    if workspace:
        keep_revs.add(repo.scm.get_rev())

    if not keep_revs:
        return 0

    delete_branches = []
    for exp_branch in repo.experiments.scm.list_branches():
        m = repo.experiments.BRANCH_RE.match(exp_branch)
        if m:
            rev = repo.scm.resolve_rev(m.group("baseline_rev"))
            if rev not in keep_revs:
                delete_branches.append(exp_branch)
    if delete_branches:
        repo.experiments.scm.repo.delete_head(*delete_branches, force=True)
    removed = len(delete_branches)

    delete_stashes = []
    for _, entry in repo.experiments.stash_revs.items():
        if not queued or entry.baseline_rev not in keep_revs:
            delete_stashes.append(entry.index)
    for index in sorted(delete_stashes, reverse=True):
        repo.experiments.scm.repo.git.stash("drop", index)
    removed += len(delete_stashes)

    return removed
