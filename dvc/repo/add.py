import os
from dvc.stage import Stage


def add(repo, fname, recursive=False, no_commit=False):
    fnames = _collect_filenames_to_add(repo, fname, recursive)

    stages = _create_stagefiles(repo, fnames, no_commit)

    repo.check_dag(repo.stages() + stages)

    for stage in stages:
        stage.dump()

    repo.remind_to_git_add()

    return stages


def _collect_filenames_to_add(repo, fname, recursive):
    if recursive and os.path.isdir(fname):
        fnames = _collect_valid_filenames_from_directory(repo, fname)
    else:
        fnames = [fname]
    return fnames


def _collect_valid_filenames_from_directory(repo, fname):
    fnames = []
    for file_path in _file_paths(fname):
        if _is_valid_file_to_add(file_path, repo):
            fnames.append(file_path)
    return fnames


def _file_paths(directory):
    for root, _, files in os.walk(directory):
        for f in files:
            yield os.path.join(root, f)


def _is_valid_file_to_add(file_path, repo):
    if Stage.is_stage_file(file_path):
        return False
    if os.path.basename(file_path) == repo.scm.ignore_file:
        return False
    if repo.scm.is_tracked(file_path):
        return False
    return True


def _create_stagefiles(repo, fnames, no_commit):
    stages = []
    with repo.state:
        for f in fnames:
            stage = Stage.create(repo=repo, outs=[f], add=True)

            if stage is None:
                continue

            stage.save()
            if not no_commit:
                stage.commit()
            stages.append(stage)
    return stages
