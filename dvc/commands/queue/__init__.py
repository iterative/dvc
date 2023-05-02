import argparse

from dvc.cli.utils import append_doc_link, fix_subparsers
from dvc.commands.queue import kill, logs, remove, start, status, stop

SUB_COMMANDS = [
    start,
    stop,
    status,
    logs,
    remove,
    kill,
]


def add_parser(subparsers, parent_parser):
    QUEUE_HELP = "Commands to manage experiments queue."

    queue_parser = subparsers.add_parser(
        "queue",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_HELP, "queue"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help=QUEUE_HELP,
    )

    queue_subparsers = queue_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc queue CMD --help` to display command-specific help.",
    )

    fix_subparsers(queue_subparsers)
    for cmd in SUB_COMMANDS:
        cmd.add_parser(queue_subparsers, parent_parser)
