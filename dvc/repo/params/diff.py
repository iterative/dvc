from dvc.utils.diff import format_dict, diff as _diff
from .show import NoParamsError


def _get_params(repo, *args, rev=None, **kwargs):
    try:
        params = repo.params.show(*args, **kwargs, revs=[rev] if rev else None)
        return params.get(rev or "", {})
    except NoParamsError:
        return {}


def diff(repo, *args, a_rev=None, b_rev=None, **kwargs):
    with_unchanged = kwargs.pop("all", False)

    old = _get_params(repo, *args, **kwargs, rev=(a_rev or "HEAD"))
    new = _get_params(repo, *args, **kwargs, rev=b_rev)

    return _diff(
        format_dict(old), format_dict(new), with_unchanged=with_unchanged
    )
