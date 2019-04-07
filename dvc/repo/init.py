import os
import shutil
import colorama
import logging

from dvc.repo import Repo
from dvc.scm import SCM, NoSCM
from dvc.config import Config
from dvc.exceptions import InitError
from dvc.utils import boxify


logger = logging.getLogger(__name__)


def _welcome_message():
    logger.info(
        boxify(
            "DVC has enabled anonymous aggregate usage analytics.\n"
            "Read the analytics documentation (and how to opt-out) here:\n"
            "{blue}https://dvc.org/doc/user-guide/analytics{nc}".format(
                blue=colorama.Fore.BLUE, nc=colorama.Fore.RESET
            ),
            border_color="red",
        )
    )

    msg = (
        "{yellow}What's next?{nc}\n"
        "{yellow}------------{nc}\n"
        "- Check out the documentation: {blue}https://dvc.org/doc{nc}\n"
        "- Get help and share ideas: {blue}https://dvc.org/chat{nc}\n"
        "- Star us on GitHub: {blue}https://github.com/iterative/dvc{nc}"
    ).format(
        yellow=colorama.Fore.YELLOW,
        blue=colorama.Fore.BLUE,
        nc=colorama.Fore.RESET,
    )

    logger.info(msg)


def init(root_dir=os.curdir, no_scm=False, force=False):
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
    root_dir = os.path.abspath(root_dir)
    dvc_dir = os.path.join(root_dir, Repo.DVC_DIR)
    scm = SCM(root_dir)
    if isinstance(scm, NoSCM) and not no_scm:
        raise InitError(
            "{repo} is not tracked by any supported scm tool (e.g. git). "
            "Use '--no-scm' if you don't want to use any scm.".format(
                repo=root_dir
            )
        )

    if os.path.isdir(dvc_dir):
        if not force:
            raise InitError(
                "'{repo}' exists. Use '-f' to force.".format(
                    repo=os.path.relpath(dvc_dir)
                )
            )

        shutil.rmtree(dvc_dir)

    os.mkdir(dvc_dir)

    config = Config.init(dvc_dir)
    proj = Repo(root_dir)

    scm.add([config.config_file])

    if scm.ignore_file:
        scm.add([os.path.join(dvc_dir, scm.ignore_file)])
        logger.info("\nYou can now commit the changes to git.\n")

    _welcome_message()

    return proj
