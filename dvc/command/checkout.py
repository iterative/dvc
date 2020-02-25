import argparse
import logging

import colorama

from dvc.command.base import append_doc_link
from dvc.command.base import CmdBase
from dvc.exceptions import CheckoutError

logger = logging.getLogger(__name__)


def _human_join(words=None):
    words = list(words or [])
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
    message = [
        ("added", "{added} added"),
        ("deleted", "{deleted} deleted"),
        ("modified", "{modified} modified"),
    ]
    summary = {
        stat: len(stats[stat]) for stat, num in message if stats.get(stat)
    }
    if not summary:
        return

    template = _human_join(
        fragment for stat, fragment in message if stat in summary
    )
    logger.info(template.format_map(summary))


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
                    state=state[0].capitalize(),
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

        if self.args.show_changes:
            log_changes(stats)
        else:
            log_summary(stats)

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
        "--show-changes",
        action="store_true",
        default=False,
        help="Show list of changes",
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
