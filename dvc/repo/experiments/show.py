import logging
from collections import defaultdict

from dvc.exceptions import DvcException
from dvc.repo import locked

logger = logging.getLogger(__name__)


@locked
def show(
    repo, all_branches=False, all_tags=False, revs=None, all_commits=False
):
    from dvc.repo.metrics.show import _collect_metrics, _read_metrics
    from dvc.repo.params.show import _collect_configs, _read_params

    res = defaultdict(dict)
    for rev in repo.brancher(
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
    ):
        configs = _collect_configs(repo)
        params = _read_params(repo, configs, rev)
        if params:
            res[rev]["params"] = params

        metrics = _collect_metrics(repo, None, False)
        vals = _read_metrics(repo, metrics, rev)
        if vals:
            res[rev]["metrics"] = vals

    if not res:
        raise DvcException("no metrics or params in this repository")

    try:
        active_branch = repo.scm.active_branch()
    except TypeError:
        pass  # Detached head
    else:
        if res.get("workspace") == res.get(active_branch):
            res.pop("workspace", None)

    return res
