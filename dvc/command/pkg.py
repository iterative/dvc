from __future__ import unicode_literals

import argparse
import logging

from dvc.pkg import PkgManager
from dvc.exceptions import DvcException
from .base import CmdBase, CmdBaseNoRepo, fix_subparsers, append_doc_link


logger = logging.getLogger(__name__)


class CmdPkgInstall(CmdBase):
    def run(self):
        try:
            self.repo.pkg.install(
                self.args.url, version=self.args.version, name=self.args.name
            )
            return 0
        except DvcException:
            logger.exception(
                "failed to install package '{}'".format(self.args.url)
            )
            return 1


class CmdPkgUninstall(CmdBase):
    def run(self):
        ret = 0
        for target in self.args.targets:
            try:
                self.repo.pkg.uninstall(target)
            except DvcException:
                logger.exception(
                    "failed to uninstall package '{}'".format(target)
                )
                ret = 1
        return ret


class CmdPkgImport(CmdBase):
    def run(self):
        try:
            self.repo.pkg.imp(
                self.args.name,
                self.args.src,
                out=self.args.out,
                version=self.args.version,
            )
            return 0
        except DvcException:
            logger.exception(
                "failed to import '{}' from package '{}'".format(
                    self.args.src, self.args.name
                )
            )
            return 1


class CmdPkgGet(CmdBaseNoRepo):
    def run(self):
        try:
            PkgManager.get(
                self.args.url,
                self.args.src,
                out=self.args.out,
                version=self.args.version,
            )
            return 0
        except DvcException:
            logger.exception(
                "failed to get '{}' from package '{}'".format(
                    self.args.src, self.args.name
                )
            )
            return 1


def add_parser(subparsers, parent_parser):
    PKG_HELP = "Manage DVC packages."
    pkg_parser = subparsers.add_parser(
        "pkg",
        parents=[parent_parser],
        description=append_doc_link(PKG_HELP, "pkg"),
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
        parents=[parent_parser],
        description=append_doc_link(PKG_INSTALL_HELP, "pkg-install"),
        help=PKG_INSTALL_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pkg_install_parser.add_argument("url", help="Package URL.")
    pkg_install_parser.add_argument(
        "--version", nargs="?", help="Package version."
    )
    pkg_install_parser.add_argument(
        "--name",
        nargs="?",
        help=(
            "Package alias. If not specified, the name will be determined "
            "from URL."
        ),
    )
    pkg_install_parser.set_defaults(func=CmdPkgInstall)

    PKG_UNINSTALL_HELP = "Uninstall package(s)."
    pkg_uninstall_parser = pkg_subparsers.add_parser(
        "uninstall",
        parents=[parent_parser],
        description=append_doc_link(PKG_UNINSTALL_HELP, "pkg-uninstall"),
        help=PKG_UNINSTALL_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pkg_uninstall_parser.add_argument(
        "targets", nargs="*", default=[None], help="Package name."
    )
    pkg_uninstall_parser.set_defaults(func=CmdPkgUninstall)

    PKG_IMPORT_HELP = "Import data from package."
    pkg_import_parser = pkg_subparsers.add_parser(
        "import",
        parents=[parent_parser],
        description=append_doc_link(PKG_IMPORT_HELP, "pkg-import"),
        help=PKG_IMPORT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pkg_import_parser.add_argument("name", help="Package name or url.")
    pkg_import_parser.add_argument("src", help="Path to data in the package.")
    pkg_import_parser.add_argument(
        "-o", "--out", nargs="?", help="Destination path to put data to."
    )
    pkg_import_parser.add_argument(
        "--version", nargs="?", help="Package version."
    )
    pkg_import_parser.set_defaults(func=CmdPkgImport)

    PKG_GET_HELP = "Download data from the package."
    pkg_get_parser = pkg_subparsers.add_parser(
        "get",
        parents=[parent_parser],
        description=append_doc_link(PKG_GET_HELP, "pkg-get"),
        help=PKG_GET_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pkg_get_parser.add_argument("url", help="Package url.")
    pkg_get_parser.add_argument("src", help="Path to data in the package.")
    pkg_get_parser.add_argument(
        "-o", "--out", nargs="?", help="Destination path to put data to."
    )
    pkg_get_parser.add_argument(
        "--version", nargs="?", help="Package version."
    )
    pkg_get_parser.set_defaults(func=CmdPkgGet)
