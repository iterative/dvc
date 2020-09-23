import logging
import os

from dvc.path_info import PathInfo
from dvc.tree.repo import RepoTree

logger = logging.getLogger(__name__)


def collect(
    repo,
    deps=False,
    targets=None,
    output_filter=None,
    rev=None,
    recursive=False,
):
    assert targets or output_filter

    outs = {
        out
        for stage in repo.stages
        for out in (stage.deps if deps else stage.outs)
    }
    if output_filter:
        outs = filter(output_filter, outs)

    if not targets:
        return outs, []

    target_infos = {PathInfo(os.path.abspath(target)) for target in targets}
    tree = RepoTree(repo)
    wrong_targets = set()
    subtargets = set()

    for t in target_infos:
        if recursive and tree.isdir(t):
            subtargets.update(set(tree.walk_files(t)))

        if not tree.isfile(t):
            if not recursive:
                logger.warning(
                    "'%s' was not found at: '%s'.", t, rev,
                )
            wrong_targets.add(t)

    target_infos = (target_infos ^ subtargets) - wrong_targets

    target_outs = set()
    for out in outs:
        if out.path_info in target_infos:
            target_outs.add(out)
            target_infos.remove(out.path_info)

    return target_outs, target_infos
