import logging
import os
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
):
    from dvc.fs.repo import RepoFileSystem
    from dvc.utils import relpath

    fs_paths = [os.path.abspath(target) for target in targets]
    fs = RepoFileSystem(repo)

    target_paths = []
    for fs_path in fs_paths:

        if recursive and fs.isdir(fs_path):
            target_paths.extend(repo.dvcignore.find(fs, fs_path))

        if not fs.exists(fs_path):
            rel = relpath(fs_path)
            if rev == "workspace" or rev == "":
                logger.warning("'%s' was not found in current workspace.", rel)
            else:
                logger.warning("'%s' was not found at: '%s'.", rel, rev)
        target_paths.append(fs_path)
    return target_paths


def _filter_duplicates(
    outs: Outputs, fs_paths: StrPaths
) -> Tuple[Outputs, StrPaths]:
    res_outs: Outputs = []
    fs_res_paths = fs_paths

    for out in outs:
        if out.fs_path in fs_paths:
            res_outs.append(out)
            # MUTATING THE SAME LIST!!
            fs_res_paths.remove(out.fs_path)

    return res_outs, fs_res_paths


def collect(
    repo: "Repo",
    deps: bool = False,
    targets: Iterable[str] = None,
    output_filter: FilterFn = None,
    rev: str = None,
    recursive: bool = False,
) -> Tuple[Outputs, StrPaths]:
    assert targets or output_filter

    outs: Outputs = _collect_outs(repo, output_filter=output_filter, deps=deps)

    if not targets:
        fs_paths: StrPaths = []
        return outs, fs_paths

    target_paths = _collect_paths(repo, targets, recursive=recursive, rev=rev)

    return _filter_duplicates(outs, target_paths)
