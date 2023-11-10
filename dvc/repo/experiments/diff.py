from dvc.log import logger
from dvc.utils.diff import diff as _diff
from dvc.utils.diff import format_dict

logger = logger.getChild(__name__)


def diff(repo, *args, a_rev=None, b_rev=None, param_deps=False, **kwargs):
    from dvc.repo.experiments.collect import collect_rev
    from dvc.scm import resolve_rev

    if repo.scm.no_commits:
        return {}

    if a_rev:
        rev = resolve_rev(repo.scm, a_rev)
    else:
        rev = resolve_rev(repo.scm, "HEAD")
    old = collect_rev(repo, rev, param_deps=param_deps)

    if b_rev:
        rev = resolve_rev(repo.scm, b_rev)
    else:
        rev = "workspace"
    new = collect_rev(repo, rev, param_deps=param_deps)

    with_unchanged = kwargs.pop("all", False)
    return {
        key: _diff(
            format_dict(getattr(old.data, key, {})),
            format_dict(getattr(new.data, key, {})),
            with_unchanged=with_unchanged,
        )
        for key in ["metrics", "params"]
    }
