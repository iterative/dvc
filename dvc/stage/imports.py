import logging

from dvc.exceptions import InvalidArgumentError

logger = logging.getLogger(__name__)


def _update_import_on_remote(stage, remote, jobs):
    if stage.is_repo_import:
        raise InvalidArgumentError(
            "Data imported from other DVC or Git repositories can't "
            "be updated with --to-remote"
        )

    url = stage.deps[0].path_info.url
    stage.outs[0].hash_info = stage.repo.cloud.transfer(
        url, jobs=jobs, remote=remote, command="update"
    )


def update_import(stage, rev=None, to_remote=False, remote=None, jobs=None):
    stage.deps[0].update(rev=rev)
    frozen = stage.frozen
    stage.frozen = False
    try:
        if to_remote:
            _update_import_on_remote(stage, remote, jobs)
        else:
            stage.reproduce(jobs=jobs)
    finally:
        stage.frozen = frozen


def get_dir_changes(stage):
    logger.debug(f"Getting changes from {stage.deps[0].path_info}")
    deps_tree = stage.deps[0].tree
    deps_path = stage.deps[0].path_info
    outs_tree = stage.outs[0].tree
    outs_path = stage.outs[0].path_info

    deps_files_dict = {
        deps_tree.get_file_hash(file).value: file
        for file in deps_tree.walk_files(deps_path)
    }
    outs_files_dict = {
        outs_tree.get_file_hash(file).value: file
        for file in outs_tree.walk_files(outs_path)
    }
    deps_files_hashes = set(deps_files_dict.keys())
    outs_files_hashes = set(outs_files_dict.keys())

    hashes_to_download = deps_files_hashes - outs_files_hashes
    hashes_to_remove = outs_files_hashes - deps_files_hashes

    files_to_download = [deps_files_dict[i] for i in hashes_to_download]
    files_to_remove = [outs_files_dict[i] for i in hashes_to_remove]
    return files_to_download, files_to_remove


def update_import_dir(stage, rev=None, jobs=None):
    stage.deps[0].update(rev=rev)
    files_to_down, files_to_rem = get_dir_changes(stage)
    logger.debug(f"Files to download: {list(files_to_down)}")
    logger.debug(f"Files to remove: {list(files_to_rem)}")

    stage.save_deps()

    for file in files_to_rem:
        stage.outs[0].tree.remove(file)

    for file in files_to_down:
        filename = file.relative_to(stage.deps[0].path_info)
        stage.deps[0].tree.download(
            file, stage.outs[0].path_info / filename, jobs=jobs
        )


def sync_import(stage, dry=False, force=False, jobs=None):
    """Synchronize import's outs to the workspace."""
    logger.info(
        "Importing '{dep}' -> '{out}'".format(
            dep=stage.deps[0], out=stage.outs[0]
        )
    )
    if dry:
        return

    if not force and stage.already_cached():
        stage.outs[0].checkout()
    else:
        stage.save_deps()
        stage.deps[0].download(stage.outs[0], jobs=jobs)
