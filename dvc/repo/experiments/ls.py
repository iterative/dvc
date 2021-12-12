import logging
from collections import defaultdict

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.types import Optional

from .utils import get_exp_ref_from_variables

logger = logging.getLogger(__name__)


@locked
@scm_context
def ls(
    repo,
    *args,
    rev: Optional[str] = None,
    git_remote: Optional[str] = None,
    all_: bool = False,
    branch: Optional[str] = None,
    **kwargs
):
    results = defaultdict(list)
    for info in get_exp_ref_from_variables(
        repo.scm, rev, all_, branch, git_remote
    ):
        results[info.baseline_sha].append(info.name)

    return results
