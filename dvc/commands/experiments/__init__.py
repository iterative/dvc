import argparse

from dvc.cli.utils import (
    append_doc_link,
    fix_plumbing_subparsers,
    fix_subparsers,
)
from dvc.commands.experiments import (
    apply,
    branch,
    diff,
    exec_run,
    gc,
    init,
    ls,
    pull,
    push,
    remove,
    run,
    show,
)

SUB_COMMANDS = [
    apply,
    branch,
    diff,
    exec_run,
    gc,
    init,
    ls,
    pull,
    push,
    remove,
    run,
    show,
]


def add_parser(subparsers, parent_parser):
    EXPERIMENTS_HELP = "Commands to run and compare experiments."

    experiments_parser = subparsers.add_parser(
        "experiments",
        parents=[parent_parser],
        aliases=["exp"],
        description=append_doc_link(EXPERIMENTS_HELP, "exp"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help=EXPERIMENTS_HELP,
    )

    experiments_subparsers = experiments_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc experiments CMD --help` to display "
        "command-specific help.",
    )

    fix_subparsers(experiments_subparsers)
    for cmd in SUB_COMMANDS:
        cmd.add_parser(experiments_subparsers, parent_parser)
    fix_plumbing_subparsers(experiments_subparsers)
