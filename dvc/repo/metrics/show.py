import os
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypedDict,
    Union,
)

from funcy import ldistinct
from scmrepo.exceptions import SCMError

from dvc.log import logger
from dvc.scm import NoSCMError
from dvc.utils import as_posix, expand_paths
from dvc.utils.collections import ensure_list
from dvc.utils.serialize import load_path

if TYPE_CHECKING:
    from dvc.fs import FileSystem
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.scm import Git, NoSCM

logger = logger.getChild(__name__)


def _collect_top_level_metrics(repo: "Repo") -> Iterator[str]:
    top_metrics = repo.index._metrics
    for dvcfile, metrics in top_metrics.items():
        wdir = repo.fs.relpath(repo.fs.parent(dvcfile), repo.root_dir)
        for file in metrics:
            path = repo.fs.join(wdir, as_posix(file))
            yield repo.fs.normpath(path)


def _extract_metrics(metrics, path: str):
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
                "Could not parse %r metric from %r due to its unsupported type: %r",
                key,
                path,
                type(val).__name__,
            )

    return ret


def _read_metric(fs: "FileSystem", path: str, **load_kwargs) -> Any:
    val = load_path(path, fs, **load_kwargs)
    val = _extract_metrics(val, path)
    return val or {}


def _read_metrics(
    fs: "FileSystem", metrics: Iterable[str], **load_kwargs
) -> Iterator[Tuple[str, Union[Exception, Any]]]:
    for metric in metrics:
        try:
            yield metric, _read_metric(fs, metric, **load_kwargs)
        except Exception as exc:  # noqa: BLE001
            logger.debug(exc)
            yield metric, exc


def metrics_from_target(repo: "Repo", targets: List[str]) -> Iterator["Output"]:
    stages = chain.from_iterable(repo.stage.collect(target) for target in targets)
    for stage in stages:
        yield from stage.metrics


def _collect_metrics(
    repo: "Repo",
    targets: Optional[List[str]] = None,
    stages: Optional[List[str]] = None,
    outs_only: bool = False,
) -> List[str]:
    metrics: List[str] = []

    if targets:
        # target is a repo-relative path
        metrics.extend(targets)

    if not targets or outs_only:
        outs = metrics_from_target(repo, stages) if stages else repo.index.metrics
        relpath = repo.fs.relpath
        metrics.extend(relpath(out.fs_path, repo.root_dir) for out in outs)

    if not targets and not outs_only and not stages:
        # _collect_top_level_metrics returns repo-relative paths
        metrics.extend(_collect_top_level_metrics(repo))

    fs = repo.dvcfs

    # convert to posixpath for DVCFileSystem
    paths = (fs.from_os_path(metric) for metric in metrics)
    # make paths absolute for DVCFileSystem
    repo_paths = (f"{fs.root_marker}{path}" for path in paths)
    return ldistinct(expand_paths(fs, repo_paths))


class FileResult(TypedDict, total=False):
    data: Any
    error: Exception


class Result(TypedDict, total=False):
    data: Dict[str, FileResult]
    error: Exception


def to_relpath(fs: "FileSystem", root_dir: str, d: Result) -> Result:
    relpath = fs.relpath
    cwd = fs.getcwd()

    start = relpath(cwd, root_dir)
    data = d.get("data")
    if data is not None:
        d["data"] = {relpath(path, start): result for path, result in data.items()}
    return d


def _gather_metrics(
    repo: "Repo",
    targets: Optional[List[str]] = None,
    outs_only: bool = False,
    stages: Optional[List[str]] = None,
    on_error: str = "return",
) -> Dict[str, FileResult]:
    assert on_error in ("raise", "return", "ignore")

    # `files` is a repo-relative posixpath that can be passed to DVCFileSystem
    # It is absolute, i.e. has a root_marker `/` in front which we strip when returning
    # the result and convert to appropriate repo-relative os.path.
    files = _collect_metrics(repo, targets=targets, stages=stages, outs_only=outs_only)
    data = {}

    fs = repo.dvcfs
    for fs_path, result in _read_metrics(fs, files, cache=True):
        repo_path = fs_path.lstrip(fs.root_marker)
        repo_os_path = os.sep.join(fs.parts(repo_path))
        if not isinstance(result, Exception):
            data.update({repo_os_path: FileResult(data=result)})
            continue

        if on_error == "raise":
            raise result
        if on_error == "return":
            data.update({repo_os_path: FileResult(error=result)})
    return data


def _hide_workspace(
    scm: Union["Git", "NoSCM"], res: Dict[str, Result]
) -> Dict[str, Result]:
    # Hide workspace params if they are the same as in the active branch
    try:
        active_branch = scm.active_branch()
    except (SCMError, NoSCMError):
        # SCMError - detached head
        # NoSCMError - no repo case
        pass
    else:
        if res.get("workspace") == res.get(active_branch):
            res.pop("workspace", None)

    return res


def show(
    repo: "Repo",
    targets: Optional[List[str]] = None,
    stages: Optional[List[str]] = None,
    outs_only: bool = False,
    all_branches: bool = False,
    all_tags: bool = False,
    revs: Optional[List[str]] = None,
    all_commits: bool = False,
    hide_workspace: bool = True,
    on_error: str = "return",
) -> Dict[str, Result]:
    assert on_error in ("raise", "return", "ignore")

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
            result = _gather_metrics(
                repo,
                targets=targets,
                stages=stages,
                outs_only=outs_only,
                on_error=on_error,
            )
            res[rev] = Result(data=result)
        except Exception as exc:  # noqa: BLE001
            if on_error == "raise":
                raise

            logger.warning("failed to load metrics in revision %r, %s", rev, str(exc))
            if on_error == "return":
                res[rev] = Result(error=exc)

    if hide_workspace:
        _hide_workspace(repo.scm, res)
    return res
