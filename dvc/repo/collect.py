import logging
from typing import TYPE_CHECKING, Callable, Iterable, List, Tuple

from dvc.types import AnyPath

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


FilterFn = Callable[["Output"], bool]
Outputs = List["Output"]
AnyPaths = List[AnyPath]
StrPaths = List[str]


def _collect_outs(
    repo: "Repo", output_filter: FilterFn = None, deps: bool = False
) -> Outputs:
    index = repo.index
    index.check_graph()  # ensure graph is correct
    return list(filter(output_filter, index.deps if deps else index.outs))


def _collect_paths(
    repo: "Repo",
    targets: Iterable[str],
    recursive: bool = False,
    rev: str = None,
) -> StrPaths:
    from dvc.fs.dvc import DvcFileSystem

    fs = DvcFileSystem(repo=repo)
    fs_paths = [fs.from_os_path(target) for target in targets]

    target_paths: StrPaths = []
    for fs_path in fs_paths:
        if recursive and fs.isdir(fs_path):
            target_paths.extend(fs.find(fs_path))

        rel = fs.path.relpath(fs_path)
        if not fs.exists(fs_path):
            if rev == "workspace" or rev == "":
                logger.warning("'%s' was not found in current workspace.", rel)
            else:
                logger.warning("'%s' was not found at: '%s'.", rel, rev)
        target_paths.append(fs_path)

    return target_paths


def _filter_outs(
    outs: Outputs, fs_paths: StrPaths, duplicates=False
) -> Tuple[Outputs, StrPaths]:
    res_outs: Outputs = []
    fs_res_paths = fs_paths

    for out in outs:
        fs_path = out.repo.dvcfs.from_os_path(out.fs_path)
        if fs_path in fs_paths:
            res_outs.append(out)
            if not duplicates:
                # MUTATING THE SAME LIST!!
                fs_res_paths.remove(fs_path)

    return res_outs, fs_res_paths


def collect(
    repo: "Repo",
    deps: bool = False,
    targets: Iterable[str] = None,
    output_filter: FilterFn = None,
    rev: str = None,
    recursive: bool = False,
    duplicates: bool = False,
) -> Tuple[Outputs, StrPaths]:
    assert targets or output_filter

    outs: Outputs = _collect_outs(repo, output_filter=output_filter, deps=deps)

    if not targets:
        fs_paths: StrPaths = []
        return outs, fs_paths

    target_paths = _collect_paths(repo, targets, recursive=recursive, rev=rev)

    return _filter_outs(outs, target_paths, duplicates=duplicates)
