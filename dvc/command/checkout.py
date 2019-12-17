from __future__ import unicode_literals

import argparse

from dvc.command.base import append_doc_link
from dvc.command.base import CmdBase


class CmdCheckout(CmdBase):
    def run(self):
        self.repo.checkout(
            targets=self.args.targets,
            with_deps=self.args.with_deps,
            force=self.args.force,
            relink=self.args.relink,
            recursive=self.args.recursive,
        )
        return 0


def add_parser(subparsers, parent_parser):
    CHECKOUT_HELP = "Checkout data files from cache."

    checkout_parser = subparsers.add_parser(
        "checkout",
        parents=[parent_parser],
        description=append_doc_link(CHECKOUT_HELP, "checkout"),
        help=CHECKOUT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    checkout_parser.add_argument(
        "-d",
        "--with-deps",
        action="store_true",
        default=False,
        help="Checkout all dependencies of the specified target.",
    )
    checkout_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Checkout all subdirectories of the specified directory.",
    )
    checkout_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Do not prompt when removing working directory files.",
    )
    checkout_parser.add_argument(
        "--relink",
        action="store_true",
        default=False,
        help="Recreate links or copies from cache to workspace.",
    )
    checkout_parser.add_argument(
        "targets",
        nargs="*",
        help="DVC-files to checkout. Optional. "
        "(Finds all DVC-files in the workspace by default.)",
    )
    checkout_parser.set_defaults(func=CmdCheckout)
