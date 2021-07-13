from dvc.repo.experiments.utils import fix_exp_head
from dvc.utils.diff import diff as _diff
from dvc.utils.diff import format_dict


def diff(repo, *args, a_rev=None, b_rev=None, **kwargs):
    if repo.scm.no_commits:
        return {}

    with_unchanged = kwargs.pop("all", False)

    a_rev = a_rev or "HEAD"
    a_rev = fix_exp_head(repo.scm, a_rev)
    b_rev = fix_exp_head(repo.scm, b_rev) or "workspace"

    params = repo.params.show(*args, **kwargs, revs=[a_rev, b_rev])

    old = params.get(a_rev, {}).get("data", {})
    new = params.get(b_rev, {}).get("data", {})

    return _diff(
        format_dict(old), format_dict(new), with_unchanged=with_unchanged
    )
