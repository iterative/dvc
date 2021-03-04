import logging

from dvc.utils.diff import diff as _diff
from dvc.utils.diff import format_dict

logger = logging.getLogger(__name__)


def diff(repo, *args, a_rev=None, b_rev=None, **kwargs):
    from dvc.repo.experiments.show import _collect_experiment_commit

    if repo.scm.no_commits:
        return {}

    if a_rev:
        rev = repo.scm.resolve_rev(a_rev)
        old = _collect_experiment_commit(repo, rev)
    else:
        old = _collect_experiment_commit(repo, "HEAD")

    if b_rev:
        rev = repo.scm.resolve_rev(b_rev)
        new = _collect_experiment_commit(repo, rev)
    else:
        new = _collect_experiment_commit(repo, "workspace")

    with_unchanged = kwargs.pop("all", False)

    return {
        key: _diff(
            format_dict(old[key]),
            format_dict(new[key]),
            with_unchanged=with_unchanged,
        )
        for key in ["metrics", "params"]
    }
