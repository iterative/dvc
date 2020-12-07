import logging
from collections import defaultdict

from dvc.repo import locked
from dvc.repo.scm_context import scm_context

from .utils import (
    exp_refs,
    exp_refs_by_baseline,
    remote_exp_refs,
    remote_exp_refs_by_baseline,
)

logger = logging.getLogger(__name__)


@locked
@scm_context
def list_(repo, *args, rev=None, git_remote=None, all_=False, **kwargs):

    if rev:
        rev = repo.scm.resolve_rev(rev)
    elif not all_:
        rev = repo.scm.get_rev()

    results = defaultdict(list)

    if rev:
        if git_remote:
            gen = remote_exp_refs_by_baseline(repo.scm, git_remote, rev)
        else:
            gen = exp_refs_by_baseline(repo.scm, rev)
        for info in gen:
            results[rev].append(info.name)
        # results[rev].extend(
        #     [info.name for info in gen]
        # )
    elif all_:
        if git_remote:
            gen = remote_exp_refs(repo.scm, git_remote)
        else:
            gen = exp_refs(repo.scm)
        for info in gen:
            results[info.baseline_sha].append(info.name)

    return results
