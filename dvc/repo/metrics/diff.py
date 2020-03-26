import json
from collections import defaultdict

from flatten_json import flatten

from dvc.exceptions import NoMetricsError


def _parse(raw):
    if raw is None or isinstance(raw, (dict, list, int, float)):
        return raw

    assert isinstance(raw, str)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _diff_vals(old, new):
    if (
        isinstance(new, list)
        and isinstance(old, list)
        and len(old) == len(new) == 1
    ):
        return _diff_vals(old[0], new[0])

    if old == new:
        return {}

    res = {"old": old, "new": new}
    if isinstance(new, (int, float)) and isinstance(old, (int, float)):
        res["diff"] = new - old
    return res


def _flatten(d):
    if not d:
        return defaultdict(lambda: None)

    if isinstance(d, dict):
        return defaultdict(lambda: None, flatten(d, "."))

    return defaultdict(lambda: "unable to parse")


def _diff_dicts(old_dict, new_dict):
    new = _flatten(new_dict)
    old = _flatten(old_dict)

    res = defaultdict(dict)

    xpaths = set(old.keys())
    xpaths.update(set(new.keys()))
    for xpath in xpaths:
        old_val = old[xpath]
        new_val = new[xpath]
        val_diff = _diff_vals(old_val, new_val)
        if val_diff:
            res[xpath] = val_diff
    return dict(res)


def _diff(old_raw, new_raw):
    old = _parse(old_raw)
    new = _parse(new_raw)

    if isinstance(new, dict) or isinstance(old, dict):
        return _diff_dicts(old, new)

    val_diff = _diff_vals(old, new)
    if val_diff:
        return {"": val_diff}

    return {}


def _get_metrics(repo, *args, rev=None, **kwargs):
    try:
        metrics = repo.metrics.show(
            *args, **kwargs, revs=[rev] if rev else None
        )
        return metrics.get(rev or "", {})
    except NoMetricsError:
        return {}


def diff(repo, *args, a_rev=None, b_rev=None, **kwargs):
    old = _get_metrics(repo, *args, **kwargs, rev=(a_rev or "HEAD"))
    new = _get_metrics(repo, *args, **kwargs, rev=b_rev)

    paths = set(old.keys())
    paths.update(set(new.keys()))

    res = defaultdict(dict)
    for path in paths:
        path_diff = _diff(old.get(path), new.get(path))
        if path_diff:
            res[path] = path_diff
    return dict(res)
