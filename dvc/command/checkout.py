import argparse
import logging

import colorama

from dvc.command.base import append_doc_link
from dvc.command.base import CmdBase
from dvc.exceptions import CheckoutError

logger = logging.getLogger(__name__)


def _human_join(words):
    words = list(words)
    if not words:
        return ""

    return (
        "{before} and {after}".format(
            before=", ".join(words[:-1]), after=words[-1],
        )
        if len(words) > 1
        else words[0]
    )


def log_summary(stats):
    states = ("added", "deleted", "modified")
    summary = (
        "{} {}".format(len(stats[state]), state)
        for state in states
        if stats.get(state)
    )
    logger.info(_human_join(summary) or "No changes.")


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
            log_summary(stats)
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
        help="DVC-files to checkout. Optional. "
        "(Finds all DVC-files in the workspace by default.)",
    )
    checkout_parser.set_defaults(func=CmdCheckout)
