import logging
from collections import defaultdict

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import iter_revs
from dvc.types import Optional

from .utils import exp_refs, exp_refs_by_baseline

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
    results = defaultdict(list)
    if all_commits:
        gen = exp_refs(repo.scm, git_remote)
        for info in gen:
            results[info.baseline_sha].append(info.name)
        return results

    rev = rev or "HEAD"

    revs = iter_revs(repo.scm, [rev], num)
    rev_set = set(revs.keys())
    ref_info_dict = exp_refs_by_baseline(repo.scm, rev_set, git_remote)
    for rev, ref_info_list in ref_info_dict.items():
        results[rev] = [ref_info.name for ref_info in ref_info_list]

    return results
