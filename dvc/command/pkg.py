from __future__ import unicode_literals

import argparse

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase, fix_subparsers, append_doc_link
from dvc.repo.pkg import PackageParams


class CmdPkg(CmdBase):
    def run(self, unlock=False):
        try:
            pkg_params = PackageParams(
                self.args.address,
                self.args.target_dir,
                self.args.select,
                self.args.file,
            )
            return self.repo.install_pkg(pkg_params)
        except DvcException:
            logger.error(
                "failed to install package '{}'".format(self.args.address)
            )
            return 1
        pass


def add_parser(subparsers, parent_parser):
    from dvc.command.config import parent_config_parser

    PKG_HELP = "Manage DVC packages."
    pkg_parser = subparsers.add_parser(
        "pkg",
        parents=[parent_parser],
        description=append_doc_link(PKG_HELP, "pkg"),
        help=PKG_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )

    pkg_subparsers = pkg_parser.add_subparsers(
        dest="cmd", help="Use dvc pkg CMD --help for command-specific help."
    )

    fix_subparsers(pkg_subparsers)

    PKG_INSTALL_HELP = "Install package."
    pkg_install_parser = pkg_subparsers.add_parser(
        "install",
        parents=[parent_config_parser, parent_parser],
        description=append_doc_link(PKG_INSTALL_HELP, "pkg-install"),
        help=PKG_INSTALL_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pkg_install_parser.add_argument(
        "address",
        nargs="?",
        default="",
        help="Package address: git://<url> or https://github.com/...",
    )
    pkg_install_parser.add_argument(
        "target_dir",
        metavar="target",
        nargs="?",
        default=".",
        help="Target directory to deploy package outputs. "
        "Default value is the current dir.",
    )
    pkg_install_parser.add_argument(
        "-s",
        "--select",
        metavar="OUT",
        action="append",
        default=[],
        help="Select and persist only specified outputs from a package. "
        "The parameter can be used multiple times. "
        "All outputs will be selected by default.",
    )
    pkg_install_parser.add_argument(
        "-f",
        "--file",
        help="Specify name of the stage file. It should be "
        "either 'Dvcfile' or have a '.dvc' suffix (e.g. "
        "'prepare.dvc', 'clean.dvc', etc). "
        "By default the file has 'mod_' prefix and imported package name "
        "followed by .dvc",
    )
    pkg_install_parser.set_defaults(func=CmdPkg)
