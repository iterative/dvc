from __future__ import unicode_literals

from dvc.command.base import CmdBase


class CmdCheckout(CmdBase):
    def run(self):
        if not self.args.targets:
            self.repo.checkout(force=self.args.force)
        else:
            for target in self.args.targets:
                self.repo.checkout(
                    target=target,
                    with_deps=self.args.with_deps,
                    force=self.args.force,
                )
        return 0


def add_parser(subparsers, parent_parser):
    CHECKOUT_HELP = (
        "Checkout data files from cache.\n"
        "documentation: https://man.dvc.org/checkout"
    )
    checkout_parser = subparsers.add_parser(
        "checkout",
        parents=[parent_parser],
        description=CHECKOUT_HELP,
        help=CHECKOUT_HELP,
    )
    checkout_parser.add_argument(
        "-d",
        "--with-deps",
        action="store_true",
        default=False,
        help="Checkout all dependencies of the specified target.",
    )
    checkout_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Do not prompt when removing working directory files.",
    )
    checkout_parser.add_argument("targets", nargs="*", help="DVC files.")
    checkout_parser.set_defaults(func=CmdCheckout)
