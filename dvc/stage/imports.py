import logging

logger = logging.getLogger(__name__)


def update_import(stage, rev=None):
    stage.deps[0].update(rev=rev)
    locked = stage.locked
    stage.locked = False
    try:
        stage.reproduce()
    finally:
        stage.locked = locked


def sync_import(stage, dry=False, force=False):
    """Synchronize import's outs to the workspace."""
    logger.info(
        "Importing '{dep}' -> '{out}'".format(
            dep=stage.deps[0], out=stage.outs[0]
        )
    )
    if dry:
        return

    if (
        not force
        and not stage.changed_stage(warn=True)
        and stage.already_cached()
    ):
        stage.outs[0].checkout()
    else:
        stage.deps[0].download(stage.outs[0])
