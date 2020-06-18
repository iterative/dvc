import logging

logger = logging.getLogger(__name__)


def update_import(stage, rev=None):
    stage.deps[0].update(rev=rev)
    frozen = stage.frozen
    stage.frozen = False
    try:
        stage.reproduce()
    finally:
        stage.frozen = frozen


def sync_import(stage, dry=False, force=False):
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
        stage.deps[0].download(stage.outs[0])
