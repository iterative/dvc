import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Tuple, Union

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import iter_revs

from .utils import exp_refs_by_baseline  # , _describe

if TYPE_CHECKING:
    from dvc.scm import Git

logger = logging.getLogger(__name__)


def _describe(
    scm: "Git",
    revs: Iterable[str],
    refs: Optional[Iterable[str]] = None,
) -> Dict[str, Optional[str]]:
    """Describe revisions using a tag, branch.

    The first matching name will be returned for each rev. Names are preferred in this
    order:
        - current branch (if rev matches HEAD and HEAD is a branch)
        - tags
        - branches

    Returns:
        Dict mapping revisions from revs to a name.
    """

    head_rev = scm.get_rev()
    head_ref = scm.get_ref("HEAD", follow=False)
    if head_ref and head_ref.startswith("refs/heads/"):
        head_branch = head_ref
    else:
        head_branch = None

    tags = {}
    branches = {}
    ref_it = iter(refs) if refs else scm.iter_refs()
    for ref in ref_it:
        is_tag = ref.startswith("refs/tags/")
        is_branch = ref.startswith("refs/heads/")
        if not (is_tag or is_branch):
            continue
        rev = scm.get_ref(ref)
        if not rev:
            logger.debug("unresolved ref %s", ref)
            continue
        if is_tag and rev not in tags:
            tags[rev] = ref
        if is_branch and rev not in branches:
            branches[rev] = ref
    names: Dict[str, Optional[str]] = {}
    for rev in revs:
        if rev == head_rev and head_branch:
            names[rev] = head_branch
        else:
            names[rev] = tags.get(rev) or branches.get(rev)
    return names


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
    baseline_names = _describe(repo.scm, revs=ref_info_dict.keys())

    results = defaultdict(list)
    for baseline in ref_info_dict:
        name = baseline
        if baseline_names[baseline]:
            # the str() retyping is a hack to deal with the typed dictionary
            # from _describe...probably there is a better way
            name = str(baseline_names.get(baseline))
        for info in ref_info_dict[baseline]:
            if git_remote:
                exp_rev = None
            else:
                exp_rev = repo.scm.get_ref(str(info))
            results[name].append((info.name, exp_rev))

    return results
