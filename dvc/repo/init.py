import logging
import os

import colorama

from dvc import analytics
from dvc.config import Config
from dvc.exceptions import InitError, InvalidArgumentError
from dvc.ignore import init as init_dvcignore
from dvc.repo import Repo
from dvc.scm import SCM
from dvc.scm.base import SCMError
from dvc.utils import boxify, relpath
from dvc.utils.fs import remove

logger = logging.getLogger(__name__)


def _welcome_message():
    if analytics.is_enabled():
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
            "{repo} is not tracked by any supported SCM tool (e.g. Git). "
            "Use `--no-scm` if you don't want to use any SCM or "
            "`--subdir` if initializing inside a subdirectory of a parent SCM "
            "repository.".format(repo=root_dir)
        )

    if os.path.isdir(dvc_dir):
        if not force:
            raise InitError(
                "'{repo}' exists. Use `-f` to force.".format(
                    repo=relpath(dvc_dir)
                )
            )

        remove(dvc_dir)

    os.mkdir(dvc_dir)

    config = Config.init(dvc_dir)

    if no_scm:
        with config.edit() as conf:
            conf["core"]["no_scm"] = True

    dvcignore = init_dvcignore(root_dir)

    proj = Repo(root_dir)

    scm.add(
        [config.files["repo"], dvcignore, proj.plot_templates.templates_dir]
    )

    if scm.ignore_file:
        scm.add([os.path.join(dvc_dir, scm.ignore_file)])
        logger.info("\nYou can now commit the changes to git.\n")

    _welcome_message()

    return proj
