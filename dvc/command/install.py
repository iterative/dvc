from __future__ import unicode_literals

import argparse

import dvc.logger as logger
from dvc.command.base import CmdBase, append_doc_link


class CmdInstall(CmdBase):
    def run_cmd(self):
        try:
            self.repo.install()
        except Exception:
            logger.error("failed to install dvc hooks")
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    INSTALL_HELP = "Install DVC git hooks into the repository."
    install_parser = subparsers.add_parser(
        "install",
        parents=[parent_parser],
        description=append_doc_link(INSTALL_HELP, "install"),
        help=INSTALL_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    install_parser.set_defaults(func=CmdInstall)
