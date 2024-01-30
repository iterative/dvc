from dvc.exceptions import InvalidArgumentError
from dvc.log import logger

logger = logger.getChild(__name__)


def _update_import_on_remote(stage, remote, jobs):
    if stage.is_repo_import:
        raise InvalidArgumentError(
            "Data imported from other DVC or Git repositories can't "
            "be updated with --to-remote"
        )

    stage.save_deps()
    url = stage.deps[0].def_path
    odb = stage.repo.cloud.get_remote_odb(remote, "update")
    stage.outs[0].transfer(url, odb=odb, jobs=jobs, update=True)


def update_import(
    stage,
    rev=None,
    to_remote=False,
    remote=None,
    no_download=None,
    jobs=None,
    force=False,
):
    stage.deps[0].update(rev=rev)

    frozen = stage.frozen
    stage.frozen = False
    changed = stage.changed()

    try:
        if to_remote:
            _update_import_on_remote(stage, remote, jobs)
        else:
            stage.reproduce(no_download=no_download, jobs=jobs, force=force)
    finally:
        if no_download and changed:
            # Avoid retaining stale information
            stage.outs[0].clear()
        stage.frozen = frozen


def sync_import(stage, dry=False, force=False, jobs=None, no_download=False):
    """Synchronize import's outs to the workspace."""
    logger.info("Importing '%s' -> '%s'", stage.deps[0], stage.outs[0])
    if dry:
        return

    if not force and stage.already_cached():
        stage.outs[0].checkout()
    else:
        stage.save_deps()
        if no_download:
            if stage.is_repo_import or stage.is_db_import:
                stage.deps[0].update()
        else:
            stage.deps[0].download(stage.outs[0], jobs=jobs)
