import logging
import os

import yaml

from dvc.exceptions import NoMetricsError
from dvc.path_info import PathInfo
from dvc.repo import locked
from dvc.repo.tree import RepoTree

logger = logging.getLogger(__name__)


def _collect_metrics(repo, targets, recursive):
    metrics = set()

    for stage in repo.stages:
        for out in stage.outs:
            if not out.metric:
                continue

            metrics.add(out.path_info)

    if not targets:
        return list(metrics)

    target_infos = [PathInfo(os.path.abspath(target)) for target in targets]

    def _filter(path_info):
        for info in target_infos:
            func = path_info.isin_or_eq if recursive else path_info.__eq__
            if func(info):
                return True
        return False

    return list(filter(_filter, metrics))


def _extract_metrics(metrics):
    if isinstance(metrics, (int, float)):
        return metrics

    if not isinstance(metrics, dict):
        return None

    ret = {}
    for key, val in metrics.items():
        m = _extract_metrics(val)
        if m:
            ret[key] = m

    return ret


def _read_metrics(repo, metrics, rev):
    tree = RepoTree(repo)

    res = {}
    for metric in metrics:
        if not tree.exists(metric):
            continue

        try:
            with tree.open(metric, "r") as fobj:
                # NOTE this also supports JSON
                val = yaml.safe_load(fobj)
        except (FileNotFoundError, yaml.YAMLError):
            logger.debug(
                "failed to read '%s' on '%s'", metric, rev, exc_info=True
            )
            continue

        val = _extract_metrics(val)
        if val:
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

    for rev in repo.brancher(
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
    ):
        metrics = _collect_metrics(repo, targets, recursive)
        vals = _read_metrics(repo, metrics, rev)

        if vals:
            res[rev] = vals

    if not res:
        raise NoMetricsError()

    # Hide workspace metrics if they are the same as in the active branch
    try:
        active_branch = repo.scm.active_branch()
    except TypeError:
        pass  # Detached head
    else:
        if res.get("workspace") == res.get(active_branch):
            res.pop("workspace", None)

    return res
