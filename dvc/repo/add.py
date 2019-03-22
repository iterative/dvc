import os

from dvc.repo.scm_context import scm_context
from dvc.stage import Stage
from dvc.utils import walk_files
from dvc.exceptions import RecursiveAddingWhileUsingFilename


@scm_context
def add(repo, target, recursive=False, no_commit=False, fname=None):
    if recursive and fname:
        raise RecursiveAddingWhileUsingFilename()

    targets = _find_all_targets(repo, target, recursive)

    stages = _create_stages(repo, targets, fname, no_commit)

    repo.check_dag(repo.stages() + stages)

    for stage in stages:
        stage.dump()
    return stages


def _find_all_targets(repo, target, recursive):
    if os.path.isdir(target) and recursive:
        return [
            file
            for file in walk_files(target)
            if not repo.is_dvc_internal(file)
            if not Stage.is_stage_file(file)
            if not repo.scm.belongs_to_scm(file)
            if not repo.scm.is_tracked(file)
        ]
    return [target]


def _create_stages(repo, targets, fname, no_commit):
    stages = []

    with repo.state:
        for out in targets:
            stage = Stage.create(repo=repo, outs=[out], add=True, fname=fname)

            if not stage:
                continue

            stage.save()

            if not no_commit:
                stage.commit()

            stages.append(stage)

    return stages
