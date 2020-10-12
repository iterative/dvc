import logging

from dvc.exceptions import NoMetricsError
from dvc.repo import locked
from dvc.repo.collect import collect
from dvc.tree.repo import RepoTree
from dvc.utils.serialize import YAMLFileCorruptedError, load_yaml

logger = logging.getLogger(__name__)


def _is_metric(out):
    return bool(out.metric)


def _collect_metrics(repo, targets, recursive):
    metrics, path_infos = collect(
        repo, targets=targets, output_filter=_is_metric, recursive=recursive
    )
    return [m.path_info for m in metrics] + list(path_infos)


def _extract_metrics(metrics, path, rev):
    if isinstance(metrics, (int, float)):
        return metrics

    if not isinstance(metrics, dict):
        return None

    ret = {}
    for key, val in metrics.items():
        m = _extract_metrics(val, path, rev)
        if m not in (None, {}):
            ret[key] = m
        else:
            logger.debug(
                "Could not parse '%s' metric from '%s' at '%s' "
                "due to its unsupported type: '%s'",
                key,
                path,
                rev,
                type(val).__name__,
            )

    return ret


def _read_metrics(repo, metrics, rev):
    tree = RepoTree(repo)

    res = {}
    for metric in metrics:
        if not tree.exists(metric):
            continue

        try:
            val = load_yaml(metric, tree=tree)
        except (FileNotFoundError, YAMLFileCorruptedError):
            logger.debug(
                "failed to read '%s' on '%s'", metric, rev, exc_info=True
            )
            continue

        val = _extract_metrics(val, metric, rev)
        if val not in (None, {}):
            res[str(metric)] = val

    return res


@locked
def show(
    repo,
    targets=None,
    all_branches=False,
    all_tags=False,
    recursive=False,
    revs=None,
    all_commits=False,
):
    res = {}
    metrics_found = False

    for rev in repo.brancher(
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
    ):
        metrics = _collect_metrics(repo, targets, recursive)

        if not metrics_found and metrics:
            metrics_found = True

        vals = _read_metrics(repo, metrics, rev)

        if vals:
            res[rev] = vals

    if not res:
        if metrics_found:
            msg = (
                "Could not parse metrics files. Use `-v` option to see more "
                "details."
            )
        else:
            msg = (
                "no metrics files in this repository. Use `-m/-M` options for "
                "`dvc run` to mark stage outputs as  metrics."
            )
        raise NoMetricsError(msg)

    # Hide workspace metrics if they are the same as in the active branch
    try:
        active_branch = repo.scm.active_branch()
    except TypeError:
        pass  # Detached head
    else:
        if res.get("workspace") == res.get(active_branch):
            res.pop("workspace", None)

    return res
