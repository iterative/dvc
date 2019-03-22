from __future__ import unicode_literals

import os

import dvc.logger as logger
from dvc.command.base import CmdBase


class CmdRoot(CmdBase):
    def run_cmd(self):
        return self.run()

    def run(self):
        logger.info(os.path.relpath(self.repo.root_dir))
        return 0


def add_parser(subparsers, parent_parser):
    ROOT_HELP = (
        "Relative path to repo's directory.\n"
        "documentation: https://man.dvc.org/root"
    )
    root_parser = subparsers.add_parser(
        "root", parents=[parent_parser], description=ROOT_HELP, help=ROOT_HELP
    )
    root_parser.set_defaults(func=CmdRoot)
