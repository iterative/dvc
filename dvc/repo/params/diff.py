import dvc.utils.diff
from .show import NoParamsError


def _get_params(repo, *args, rev=None, **kwargs):
    try:
        params = repo.params.show(*args, **kwargs, revs=[rev] if rev else None)
        return params.get(rev or "", {})
    except NoParamsError:
        return {}


def _format(params):
    ret = {}
    for key, val in params.items():
        if isinstance(val, dict):
            new_val = _format(val)
        elif isinstance(val, list):
            new_val = str(val)
        else:
            new_val = val
        ret[key] = new_val
    return ret


def diff(repo, *args, a_rev=None, b_rev=None, **kwargs):
    old = _get_params(repo, *args, **kwargs, rev=(a_rev or "HEAD"))
    new = _get_params(repo, *args, **kwargs, rev=b_rev)

    return dvc.utils.diff.diff(_format(old), _format(new))
