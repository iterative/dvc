import logging
import os
from typing import List

from scmrepo.exceptions import SCMError

from dvc.fs.repo import RepoFileSystem
from dvc.output import Output
from dvc.repo import locked
from dvc.repo.collect import StrPaths, collect
from dvc.repo.live import summary_fs_path
from dvc.scm import NoSCMError
from dvc.utils import error_handler, errored_revisions, onerror_collect
from dvc.utils.serialize import load_yaml

logger = logging.getLogger(__name__)


def _is_metric(out: Output) -> bool:
    return bool(out.metric) or bool(out.live)


def _to_fs_paths(metrics: List[Output]) -> StrPaths:
    result = []
    for out in metrics:
        if out.metric:
            result.append(out.fs_path)
        elif out.live:
            fs_path = summary_fs_path(out)
            if fs_path:
                result.append(fs_path)
    return result


def _collect_metrics(repo, targets, revision, recursive):
    metrics, fs_paths = collect(
        repo,
        targets=targets,
        output_filter=_is_metric,
        recursive=recursive,
        rev=revision,
    )
    return _to_fs_paths(metrics) + list(fs_paths)


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


@error_handler
def _read_metric(path, fs, rev, **kwargs):
    val = load_yaml(path, fs=fs)
    val = _extract_metrics(val, path, rev)
    return val or {}


def _read_metrics(repo, metrics, rev, onerror=None):
    fs = RepoFileSystem(repo)

    res = {}
    for metric in metrics:
        if not fs.isfile(metric):
            continue

        res[fs.path.relpath(metric, os.getcwd())] = _read_metric(
            metric, fs, rev, onerror=onerror
        )

    return res


def _gather_metrics(repo, targets, rev, recursive, onerror=None):
    metrics = _collect_metrics(repo, targets, rev, recursive)
    return _read_metrics(repo, metrics, rev, onerror=onerror)


@locked
def show(
    repo,
    targets=None,
    all_branches=False,
    all_tags=False,
    recursive=False,
    revs=None,
    all_commits=False,
    onerror=None,
):
    if onerror is None:
        onerror = onerror_collect

    res = {}
    for rev in repo.brancher(
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
    ):
        res[rev] = error_handler(_gather_metrics)(
            repo, targets, rev, recursive, onerror=onerror
        )

    # Hide workspace metrics if they are the same as in the active branch
    try:
        active_branch = repo.scm.active_branch()
    except (SCMError, NoSCMError):
        # SCMError - detached head
        # NoSCMError - no repo case
        pass
    else:
        if res.get("workspace") == res.get(active_branch):
            res.pop("workspace", None)

    errored = errored_revisions(res)
    if errored:
        from dvc.ui import ui

        ui.error_write(
            "DVC failed to load some metrics for following revisions:"
            f" '{', '.join(errored)}'."
        )

    return res
