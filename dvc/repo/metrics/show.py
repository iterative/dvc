import logging
import os
from typing import TYPE_CHECKING, List

from scmrepo.exceptions import SCMError

from dvc.fs.dvc import DVCFileSystem
from dvc.repo import locked
from dvc.repo.collect import collect
from dvc.scm import NoSCMError
from dvc.utils import as_posix, error_handler, errored_revisions, onerror_collect
from dvc.utils.collections import ensure_list
from dvc.utils.serialize import load_path

if TYPE_CHECKING:
    from dvc.output import Output

logger = logging.getLogger(__name__)


def _is_metric(out: "Output") -> bool:
    return bool(out.metric)


def _to_fs_paths(metrics: List["Output"]) -> List["str"]:
    result = []
    for out in metrics:
        if out.metric:
            result.append(out.repo.dvcfs.from_os_path(out.fs_path))
    return result


def _collect_top_level_metrics(repo):
    top_metrics = repo.index._metrics  # pylint: disable=protected-access
    for dvcfile, metrics in top_metrics.items():
        wdir = repo.fs.path.relpath(repo.fs.path.parent(dvcfile), repo.root_dir)
        for file in metrics:
            path = repo.fs.path.join(wdir, as_posix(file))
            yield repo.fs.path.normpath(path)


def _collect_metrics(repo, targets, recursive):
    metrics, fs_paths = collect(
        repo, targets=targets, output_filter=_is_metric, recursive=recursive
    )
    return _to_fs_paths(metrics) + list(fs_paths)


def _extract_metrics(metrics, path, rev):
    if isinstance(metrics, (int, float, str)):
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
                (
                    "Could not parse '%s' metric from '%s' at '%s' "
                    "due to its unsupported type: '%s'"
                ),
                key,
                path,
                rev,
                type(val).__name__,
            )

    return ret


@error_handler
def _read_metric(path, fs, rev, **kwargs):
    val = load_path(path, fs)
    val = _extract_metrics(val, path, rev)
    return val or {}


def _read_metrics(repo, metrics, rev, onerror=None):
    fs = DVCFileSystem(repo=repo)

    relpath = ""
    if repo.root_dir != repo.fs.path.getcwd():
        relpath = repo.fs.path.relpath(repo.root_dir, repo.fs.path.getcwd())

    res = {}
    for metric in metrics:
        rel_metric_path = os.path.join(relpath, *fs.path.parts(metric))
        if not fs.isfile(metric):
            if fs.isfile(rel_metric_path):
                metric = rel_metric_path
            else:
                continue

        res[rel_metric_path] = _read_metric(metric, fs, rev, onerror=onerror)

    return res


def _gather_metrics(repo, targets, rev, recursive, onerror=None):
    metrics = _collect_metrics(repo, targets, recursive)
    metrics.extend(_collect_top_level_metrics(repo))
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
    hide_workspace=True,
):
    if onerror is None:
        onerror = onerror_collect

    targets = ensure_list(targets)
    targets = [repo.dvcfs.from_os_path(target) for target in targets]

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

    if hide_workspace:
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
