from __future__ import unicode_literals

import dvc.logger as logger
from dvc.command.base import CmdBase


class CmdInstall(CmdBase):
    def run_cmd(self):
        try:
            self.repo.install()
        except Exception:
            logger.error("failed to install dvc hooks")
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    INSTALL_HELP = (
        "Install dvc hooks into the repository.\n"
        "documentation: https://man.dvc.org/install"
    )
    install_parser = subparsers.add_parser(
        "install",
        parents=[parent_parser],
        description=INSTALL_HELP,
        help=INSTALL_HELP,
    )
    install_parser.set_defaults(func=CmdInstall)
