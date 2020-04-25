from dvc.utils.diff import format_dict, diff as _diff
from dvc.exceptions import NoMetricsError


def _get_metrics(repo, *args, rev=None, **kwargs):
    try:
        metrics = repo.metrics.show(
            *args, **kwargs, revs=[rev] if rev else None
        )
        return metrics.get(rev or "", {})
    except NoMetricsError:
        return {}


def diff(repo, *args, a_rev=None, b_rev=None, **kwargs):
    with_unchanged = kwargs.pop("all", False)

    old = _get_metrics(repo, *args, **kwargs, rev=(a_rev or "HEAD"))
    new = _get_metrics(repo, *args, **kwargs, rev=b_rev)

    return _diff(
        format_dict(old), format_dict(new), with_unchanged=with_unchanged
    )
