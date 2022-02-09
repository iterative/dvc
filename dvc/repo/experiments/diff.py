import logging

from dvc.utils.diff import diff as _diff
from dvc.utils.diff import format_dict

logger = logging.getLogger(__name__)


def diff(repo, *args, a_rev=None, b_rev=None, param_deps=False, **kwargs):
    from dvc.repo.experiments.show import _collect_experiment_commit
    from dvc.scm import resolve_rev

    if repo.scm.no_commits:
        return {}

    if a_rev:
        rev = resolve_rev(repo.scm, a_rev)
        old = _collect_experiment_commit(repo, rev, param_deps=param_deps)
    else:
        old = _collect_experiment_commit(repo, "HEAD", param_deps=param_deps)

    if b_rev:
        rev = resolve_rev(repo.scm, b_rev)
        new = _collect_experiment_commit(repo, rev, param_deps=param_deps)
    else:
        new = _collect_experiment_commit(
            repo, "workspace", param_deps=param_deps
        )

    with_unchanged = kwargs.pop("all", False)

    return {
        key: _diff(
            format_dict(old.get("data", {}).get(key, {})),
            format_dict(new.get("data", {}).get(key, {})),
            with_unchanged=with_unchanged,
        )
        for key in ["metrics", "params"]
    }
