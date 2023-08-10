import logging
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import iter_revs

from .utils import exp_refs_by_baseline

logger = logging.getLogger(__name__)


@locked
@scm_context
def ls(
    repo,
    rev: Optional[Union[List[str], str]] = None,
    all_commits: bool = False,
    num: int = 1,
    git_remote: Optional[str] = None,
) -> Dict[str, List[Tuple[str, Optional[str]]]]:
    """List experiments.

    Returns a dict mapping baseline revs to a list of (exp_name, exp_sha) tuples.
    """
    rev_set = None
    if not all_commits:
        rev = rev or "HEAD"
        if isinstance(rev, str):
            rev = [rev]
        revs = iter_revs(repo.scm, rev, num)
        rev_set = set(revs.keys())

    ref_info_dict = exp_refs_by_baseline(repo.scm, rev_set, git_remote)
    results = defaultdict(list)
    for baseline in ref_info_dict:
        for info in ref_info_dict[baseline]:
            if git_remote:
                exp_rev = None
            else:
                exp_rev = repo.scm.get_ref(str(info))
            results[baseline].append((info.name, exp_rev))

    return results
