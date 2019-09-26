from __future__ import unicode_literals

import os
import logging
import colorama

from dvc.repo.scm_context import scm_context
from dvc.stage import Stage
from dvc.utils import walk_files, LARGE_DIR_SIZE
from dvc.exceptions import RecursiveAddingWhileUsingFilename

from . import locked


logger = logging.getLogger(__name__)


@locked
@scm_context
def add(repo, target, recursive=False, no_commit=False, fname=None):
    if recursive and fname:
        raise RecursiveAddingWhileUsingFilename()

    targets = _find_all_targets(repo, target, recursive)

    if os.path.isdir(target) and len(targets) > LARGE_DIR_SIZE:
        logger.warning(
            "You are adding a large directory '{target}' recursively,"
            " consider tracking it as a whole instead.\n"
            "{purple}HINT:{nc} Remove the generated DVC-file and then"
            " run {cyan}dvc add {target}{nc}".format(
                purple=colorama.Fore.MAGENTA,
                cyan=colorama.Fore.CYAN,
                nc=colorama.Style.RESET_ALL,
                target=target,
            )
        )

    with repo.state:
        stages = _create_stages(repo, targets, fname)

        repo.check_modified_graph(stages)

        for stage in stages:
            stage.save()

            if not no_commit:
                stage.commit()

            stage.dump()

    return stages


def _find_all_targets(repo, target, recursive):
    if os.path.isdir(target) and recursive:
        return [
            fname
            for fname in walk_files(target, repo.dvcignore)
            if not repo.is_dvc_internal(fname)
            if not Stage.is_stage_file(fname)
            if not repo.scm.belongs_to_scm(fname)
            if not repo.scm.is_tracked(fname)
        ]
    return [target]


def _create_stages(repo, targets, fname):
    stages = []

    for out in targets:
        stage = Stage.create(repo, outs=[out], add=True, fname=fname)

        if not stage:
            continue

        stages.append(stage)

    return stages
