import logging
import os

from ..config import Config
from ..exceptions import InitError, InvalidArgumentError
from ..ignore import init as init_dvcignore
from ..scm import SCM, SCMError
from ..utils import relpath
from ..utils.fs import remove
from . import Repo

logger = logging.getLogger(__name__)


def init(root_dir=os.curdir, no_scm=False, force=False, subdir=False):
    """
    Creates an empty repo on the given directory -- basically a
    `.dvc` directory with subdirectories for configuration and cache.

    It should be tracked by a SCM or use the `--no-scm` flag.

    If the given directory is not empty, you must use the `--force`
    flag to override it.

    Args:
        root_dir: Path to repo's root directory.

    Returns:
        Repo instance.

    Raises:
        KeyError: Raises an exception.
    """

    if no_scm and subdir:
        raise InvalidArgumentError(
            "Cannot initialize repo with `--no-scm` and `--subdir`"
        )

    root_dir = os.path.realpath(root_dir)
    dvc_dir = os.path.join(root_dir, Repo.DVC_DIR)

    try:
        scm = SCM(root_dir, search_parent_directories=subdir, no_scm=no_scm)
    except SCMError:
        raise InitError(
            f"{root_dir} is not tracked by any supported SCM tool (e.g. Git). "
            "Use `--no-scm` if you don't want to use any SCM or "
            "`--subdir` if initializing inside a subdirectory of a parent SCM "
            "repository."
        )

    if scm.is_ignored(dvc_dir):
        raise InitError(
            f"{dvc_dir} is ignored by your SCM tool. \n"
            "Make sure that it's tracked, "
            "for example, by adding '!.dvc' to .gitignore."
        )

    if os.path.isdir(dvc_dir):
        if not force:
            raise InitError(f"'{relpath(dvc_dir)}' exists. Use `-f` to force.")

        remove(dvc_dir)

    os.mkdir(dvc_dir)

    config = Config.init(dvc_dir)

    if no_scm:
        with config.edit() as conf:
            conf["core"]["no_scm"] = True

    dvcignore = init_dvcignore(root_dir)

    proj = Repo(root_dir)

    with proj.scm_context(autostage=True) as context:
        files = [
            config.files["repo"],
            dvcignore,
        ]
        ignore_file = context.scm.ignore_file
        if ignore_file:
            files.extend([os.path.join(dvc_dir, ignore_file)])
        proj.scm_context.track_file(files)

    logger.info("Initialized DVC repository.\n")
    if not no_scm:
        logger.info("You can now commit the changes to git.\n")
    return proj
