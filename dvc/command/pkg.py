from __future__ import unicode_literals

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase, fix_subparsers


class CmdPkg(CmdBase):
    def run(self, unlock=False):
        try:
            return self.repo.install_pkg(self.args.address,
                                         self.args.target_dir,
                                         self.args.outs,
                                         self.args.file)
        except DvcException:
            logger.error(
                "failed to install package '{}'".format(self.args.address)
            )
            return 1
        pass


def add_parser(subparsers, parent_parser):
    from dvc.command.config import parent_config_parser

    PKG_HELP = "Manage packages and modules"
    pkg_parser = subparsers.add_parser(
        "pkg",
        parents=[parent_parser],
        description=PKG_HELP,
        help=PKG_HELP,
    )

    pkg_subparsers = pkg_parser.add_subparsers(
        dest="cmd",
        help="Use dvc pkg CMD --help for command-specific help.",
    )

    fix_subparsers(pkg_subparsers)

    PKG_INSTALL_HELP = "Install package."
    pkg_install_parser = pkg_subparsers.add_parser(
        "install",
        parents=[parent_config_parser, parent_parser],
        description=PKG_INSTALL_HELP,
        help=PKG_INSTALL_HELP,
    )
    pkg_install_parser.add_argument(
        "address",
        nargs="?",
        default="",
        help="Package address: git://<url> or https://github.com/..."
    )
    pkg_install_parser.add_argument(
        "target_dir",
        nargs="?",
        default=".",
        help="Target directory to deploy package outputs"
    )
    pkg_install_parser.add_argument(
        "-o",
        "--outs",
        action="append",
        default=[],
        help="deploy package output",
    )
    pkg_install_parser.add_argument(
        "-f",
        "--file",
        help="Specify name of the stage file. It should be "
        "either 'Dvcfile' or have a '.dvc' suffix (e.g. "
        "'prepare.dvc', 'clean.dvc', etc) in order for "
        "dvc to be able to find it later. By default "
        "the file has 'mod_' prefix and imported package name "
        "followed by .dvc",
    )
    pkg_install_parser.set_defaults(func=CmdPkg)
