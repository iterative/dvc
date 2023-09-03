import logging
import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypedDict

from funcy import ldistinct
from scmrepo.exceptions import SCMError

from dvc.repo import locked
from dvc.scm import NoSCMError
from dvc.utils import as_posix
from dvc.utils.collections import ensure_list
from dvc.utils.serialize import load_path

if TYPE_CHECKING:
    from dvc.fs import FileSystem
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


def _collect_top_level_metrics(repo):
    top_metrics = repo.index._metrics  # pylint: disable=protected-access
    for dvcfile, metrics in top_metrics.items():
        wdir = repo.fs.path.relpath(repo.fs.path.parent(dvcfile), repo.root_dir)
        for file in metrics:
            path = repo.fs.path.join(wdir, as_posix(file))
            yield repo.fs.path.normpath(path)


def _extract_metrics(metrics, path):
    if isinstance(metrics, (int, float, str)):
        return metrics

    if not isinstance(metrics, dict):
        return None

    ret = {}
    for key, val in metrics.items():
        m = _extract_metrics(val, path)
        if m not in (None, {}):
            ret[key] = m
        else:
            logger.debug(
                (
                    "Could not parse '%s' metric from '%s'"
                    "due to its unsupported type: '%s'"
                ),
                key,
                path,
                type(val),
            )

    return ret


def _read_metric(path, fs):
    val = load_path(path, fs)
    val = _extract_metrics(val, path)
    return val or {}


def _read_metrics(fs, metrics):
    for metric in metrics:
        try:
            yield metric, _read_metric(metric, fs)
        except Exception as exc:  # noqa: BLE001 # pylint:disable=broad-exception-caught
            logger.debug(exc)
            yield metric, exc


def expand_paths(dvcfs, paths):
    for path in paths:
        abspath = f"{os.sep}{path}"
        if dvcfs.isdir(abspath):
            yield from dvcfs.find(abspath)
        else:
            yield abspath


def _collect_metrics(repo, targets: Optional[List[str]]) -> List[str]:
    dvcfs = repo.dvcfs
    if targets:
        metrics = list(targets)
    else:
        metrics = [dvcfs.from_os_path(out.fs_path) for out in repo.index.metrics]
        metrics.extend(_collect_top_level_metrics(repo))

    return ldistinct(expand_paths(dvcfs, metrics))


class FileResult(TypedDict, total=False):
    data: Any
    error: Exception


class Result(TypedDict, total=False):
    data: Dict[str, FileResult]
    error: Exception


def to_relpath(fs: "FileSystem", root_dir: str, d: Result) -> Result:
    relpath = fs.path.relpath
    cwd = fs.path.getcwd()

    start = relpath(cwd, root_dir)
    data = d.get("data")
    if data is not None:
        d["data"] = {relpath(path, start): result for path, result in data.items()}
    return d


def _show(
    repo: "Repo",
    targets: Optional[List[str]] = None,
    on_error: str = "return",
) -> Dict[str, FileResult]:
    assert on_error in ("raise", "return")

    files = _collect_metrics(repo, targets=targets)
    data = {}
    for file, result in _read_metrics(repo.dvcfs, files):
        repo_path = file.lstrip(os.sep)
        if not isinstance(result, Exception):
            data.update({repo_path: FileResult(data=result)})
            continue

        if on_error == "raise":
            raise result
        data.update({repo_path: FileResult(error=result)})
    return data


@locked
def show(
    repo: "Repo",
    targets: Optional[List[str]] = None,
    all_branches=False,
    all_tags=False,
    revs=None,
    all_commits=False,
    hide_workspace=True,
    on_error: str = "return",
) -> Dict[str, Result]:
    targets = [os.path.abspath(target) for target in ensure_list(targets)]
    targets = [repo.dvcfs.from_os_path(target) for target in targets]

    res = {}
    for rev in repo.brancher(
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
    ):
        try:
            result = _show(repo, targets=targets, on_error=on_error)
            res[rev] = Result(data=result)
        except Exception as exc:  # noqa: BLE001 # pylint:disable=broad-exception-caught
            if on_error == "raise":
                raise

            logger.debug("failed to load in revision %r, %s", rev, str(exc))
            res[rev] = Result(error=exc)

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

    return res
