import logging
import os
from typing import Iterable

from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.tree.repo import RepoTree

logger = logging.getLogger(__name__)


def _collect_outs(
    repo: Repo, output_filter: callable = None, deps: bool = False
):
    outs = {
        out
        for stage in repo.stages
        for out in (stage.deps if deps else stage.outs)
    }
    return set(filter(output_filter, outs)) if output_filter else outs


def _collect_paths(
    repo: Repo, targets: Iterable, recursive: bool = False, rev: str = None
):
    path_infos = {PathInfo(os.path.abspath(target)) for target in targets}
    tree = RepoTree(repo)

    target_infos = set()
    for path_info in path_infos:

        if recursive and tree.isdir(path_info):
            target_infos.update(set(tree.walk_files(path_info)))

        if not tree.isfile(path_info):
            if not recursive:
                logger.warning(
                    "'%s' was not found at: '%s'.", path_info, rev,
                )
            continue
        target_infos.add(path_info)
    return target_infos


def _filter_duplicates(outs: Iterable, path_infos: Iterable):
    res_outs = set()
    res_infos = set(path_infos)

    for out in outs:
        if out.path_info in path_infos:
            res_outs.add(out)
            res_infos.remove(out.path_info)

    return res_outs, res_infos


def collect(
    repo: Repo,
    deps: bool = False,
    targets: Iterable = None,
    output_filter: callable = None,
    rev: str = None,
    recursive: bool = False,
):
    assert targets or output_filter

    outs = _collect_outs(repo, output_filter=output_filter, deps=deps)

    if not targets:
        return outs, set()

    target_infos = _collect_paths(repo, targets, recursive=recursive, rev=rev)

    return _filter_duplicates(outs, target_infos)
