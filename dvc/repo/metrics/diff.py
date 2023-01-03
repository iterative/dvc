from dvc.utils.diff import diff as _diff
from dvc.utils.diff import format_dict


def diff(repo, *args, a_rev=None, b_rev=None, **kwargs):
    if repo.scm.no_commits:
        return {}

    with_unchanged = kwargs.pop("all", False)

    a_rev = a_rev or "HEAD"
    b_rev = b_rev or "workspace"

    metrics = repo.metrics.show(*args, **kwargs, revs=[a_rev, b_rev])

    # workspace may have been replaced by active branch
    workspace_rev = (a_rev == "workspace") or (b_rev == "workspace")
    if workspace_rev and "workspace" not in metrics:
        active_branch = repo.scm.active_branch()
        if active_branch in metrics:
            metrics["workspace"] = metrics[active_branch]

    old = metrics.get(a_rev, {}).get("data", {})
    new = metrics.get(b_rev, {}).get("data", {})

    return _diff(
        format_dict(old), format_dict(new), with_unchanged=with_unchanged
    )
