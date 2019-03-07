import os

from dvc.scm import scm_context
from dvc.stage import Stage
from dvc.utils import walk_files
from dvc.exceptions import RecursiveAddingWhileUsingFilename


@scm_context
def add(repo, target, recursive=False, no_commit=False, fname=None):
    if recursive and fname:
        raise RecursiveAddingWhileUsingFilename()

    targets = [target]

    if os.path.isdir(target) and recursive:
        targets = [
            file
            for file in walk_files(target)
            if not Stage.is_stage_file(file)
            if os.path.basename(file) != repo.scm.ignore_file
            if not repo.scm.is_tracked(file)
        ]

    stages = _create_stages(repo, targets, fname, no_commit)

    repo.check_dag(repo.stages() + stages)

    for stage in stages:
        stage.dump()
    return stages


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
