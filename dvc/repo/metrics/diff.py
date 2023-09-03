from dvc.utils.diff import diff as _diff
from dvc.utils.diff import format_dict


def diff(repo, *args, a_rev=None, b_rev=None, **kwargs):
    from .show import to_relpath

    if repo.scm.no_commits:
        return {}

    with_unchanged = kwargs.pop("all", False)

    a_rev = a_rev or "HEAD"
    b_rev = b_rev or "workspace"

    metrics = repo.metrics.show(
        *args,
        **kwargs,
        revs=[a_rev, b_rev],
        hide_workspace=False,
        on_error="return",
    )
    metrics = {k: to_relpath(repo.fs, repo.root_dir, v) for k, v in metrics.items()}
    old = metrics.get(a_rev, {}).get("data", {})
    new = metrics.get(b_rev, {}).get("data", {})

    return _diff(format_dict(old), format_dict(new), with_unchanged=with_unchanged)
