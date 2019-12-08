from __future__ import unicode_literals

import logging


logger = logging.getLogger(__name__)


def fix_subparsers(subparsers):
    """Workaround for bug in Python 3. See more info at:
        https://bugs.python.org/issue16308
        https://github.com/iterative/dvc/issues/769

        Args:
            subparsers: subparsers to fix.
    """
    from dvc.utils.compat import is_py3

    if is_py3:  # pragma: no cover
        subparsers.required = True
        subparsers.dest = "cmd"


def append_doc_link(help_message, path):
    from dvc.utils import format_link

    if not path:
        return help_message
    doc_base = "https://man.dvc.org/"
    return "{message}\nDocumentation: {link}".format(
        message=help_message, link=format_link(doc_base + path)
    )


class CmdBase(object):
    def __init__(self, args):
        from dvc.repo import Repo
        from dvc.updater import Updater

        self.repo = Repo()
        self.config = self.repo.config
        self.args = args
        hardlink_lock = self.config.config["core"].get("hardlink_lock", False)
        updater = Updater(self.repo.dvc_dir, hardlink_lock=hardlink_lock)
        updater.check()

    @property
    def default_targets(self):
        """Default targets for `dvc repro` and `dvc pipeline`."""
        from dvc.stage import Stage

        msg = "assuming default target '{}'.".format(Stage.STAGE_FILE)
        logger.warning(msg)
        return [Stage.STAGE_FILE]

    # Abstract methods that have to be implemented by any inheritance class
    def run(self):
        raise NotImplementedError


class CmdBaseNoRepo(CmdBase):
    def __init__(self, args):
        self.args = args
