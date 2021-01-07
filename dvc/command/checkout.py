import argparse
import logging
import operator

import colorama

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import CheckoutError

logger = logging.getLogger(__name__)


def log_changes(stats):
    colors = [
        ("modified", colorama.Fore.YELLOW,),
        ("added", colorama.Fore.GREEN),
        ("deleted", colorama.Fore.RED,),
    ]

    for state, color in colors:
        entries = stats.get(state)

        if not entries:
            continue

        for entry in entries:
            logger.info(
                "{color}{state}{nc}{spacing}{entry}".format(
                    color=color,
                    state=state[0].upper(),
                    nc=colorama.Fore.RESET,
                    spacing="\t",
                    entry=entry,
                )
            )


class CmdCheckout(CmdBase):
    def run(self):
        from dvc.utils.humanize import get_summary

        stats, exc = None, None
        try:
            stats = self.repo.checkout(
                targets=self.args.targets,
                with_deps=self.args.with_deps,
                force=self.args.force,
                relink=self.args.relink,
                recursive=self.args.recursive,
            )
        except CheckoutError as _exc:
            exc = _exc
            stats = exc.stats

        if self.args.summary:
            default_message = "No changes."
            msg = get_summary(
                sorted(stats.items(), key=operator.itemgetter(0))
            )
            logger.info(msg or default_message)
        else:
            log_changes(stats)

        if exc:
            raise exc
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
        "--summary",
        action="store_true",
        default=False,
        help="Show summary of the changes.",
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
        help=(
            "Limit command scope to these tracked files/directories, "
            ".dvc files, or stage names."
        ),
    ).complete = completion.DVC_FILE
    checkout_parser.set_defaults(func=CmdCheckout)
