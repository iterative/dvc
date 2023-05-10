import logging
from collections import defaultdict
from typing import Optional

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import iter_revs

from .utils import exp_refs_by_baseline

logger = logging.getLogger(__name__)


@locked
@scm_context
def ls(
    repo,
    rev: Optional[str] = None,
    all_commits: bool = False,
    num: int = 1,
    git_remote: Optional[str] = None,
):
    rev_set = None
    if not all_commits:
        revs = iter_revs(repo.scm, [rev or "HEAD"], num)
        rev_set = set(revs.keys())
    ref_info_dict = exp_refs_by_baseline(repo.scm, rev_set, git_remote)

    tags = repo.scm.describe(ref_info_dict.keys())
    remained = {baseline for baseline, tag in tags.items() if tag is None}
    base = "refs/heads"
    ref_heads = repo.scm.describe(remained, base=base)

    results = defaultdict(list)
    for baseline in ref_info_dict:
        name = baseline[:7]
        if tags[baseline] or ref_heads[baseline]:
            name = tags[baseline] or ref_heads[baseline][len(base) + 1 :]
        results[name] = [info.name for info in ref_info_dict[baseline]]

    return results
