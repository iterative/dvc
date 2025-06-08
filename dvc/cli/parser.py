"""Main parser for the dvc cli."""

import argparse
import os
from functools import lru_cache

from dvc import __version__
from dvc.commands import (
    add,
    artifacts,
    cache,
    check_ignore,
    checkout,
    commit,
    completion,
    config,
    daemon,
    dag,
    data,
    data_sync,
    dataset,
    destroy,
    diff,
    du,
    experiments,
    freeze,
    gc,
    get,
    get_url,
    git_hook,
    imp,
    imp_db,
    imp_url,
    init,
    install,
    ls,
    ls_url,
    metrics,
    move,
    params,
    plots,
    queue,
    remote,
    remove,
    repro,
    root,
    stage,
    studio,
    unprotect,
    update,
    version,
)
from dvc.log import logger

from . import DvcParserError, formatter

logger = logger.getChild(__name__)

COMMANDS = [
    add,
    artifacts,
    cache,
    check_ignore,
    checkout,
    commit,
    completion,
    config,
    daemon,
    dag,
    data,
    data_sync,
    dataset,
    destroy,
    diff,
    du,
    experiments,
    freeze,
    gc,
    get,
    get_url,
    git_hook,
    imp,
    imp_db,
    imp_url,
    init,
    install,
    ls,
    ls_url,
    metrics,
    move,
    params,
    plots,
    queue,
    remote,
    remove,
    repro,
    root,
    stage,
    studio,
    unprotect,
    update,
    version,
]


def _find_parser(parser, cmd_cls):
    defaults = parser._defaults
    if not cmd_cls or cmd_cls == defaults.get("func"):
        parser.print_help()
        raise DvcParserError

    actions = parser._actions
    for action in actions:
        if not isinstance(action.choices, dict):
            # NOTE: we are only interested in subparsers
            continue
        for subparser in action.choices.values():
            _find_parser(subparser, cmd_cls)


class DvcParser(argparse.ArgumentParser):
    """Custom parser class for dvc CLI."""

    def error(self, message, cmd_cls=None):
        logger.error(message)
        _find_parser(self, cmd_cls)

    def parse_args(self, args=None, namespace=None):
        # NOTE: overriding to provide a more granular help message.
        # E.g. `dvc plots diff --bad-flag` would result in a `dvc plots diff`
        # help message instead of generic `dvc` usage.
        args, argv = self.parse_known_args(args, namespace)
        if argv:
            msg = "unrecognized arguments: %s"
            self.error(msg % " ".join(argv), getattr(args, "func", None))
        return args


def get_parent_parser():
    """Create instances of a parser containing common arguments shared among
    all the commands.

    When overwriting `-q` or `-v`, you need to instantiate a new object
    in order to prevent some weird behavior.
    """
    from dvc._debug import add_debugging_flags

    parent_parser = argparse.ArgumentParser(add_help=False)
    log_level_group = parent_parser.add_mutually_exclusive_group()
    log_level_group.add_argument(
        "-q", "--quiet", action="count", default=0, help="Be quiet."
    )
    log_level_group.add_argument(
        "-v", "--verbose", action="count", default=0, help="Be verbose."
    )
    add_debugging_flags(parent_parser)

    return parent_parser


@lru_cache(maxsize=1)
def get_main_parser():
    parent_parser = get_parent_parser()

    # Main parser
    desc = "Data Version Control"
    parser = DvcParser(
        prog="dvc",
        description=desc,
        parents=[parent_parser],
        formatter_class=formatter.RawTextHelpFormatter,
        add_help=False,
    )

    # NOTE: We are doing this to capitalize help message.
    # Unfortunately, there is no easier and clearer way to do it,
    # as adding this argument in get_parent_parser() either in
    # log_level_group or on parent_parser itself will cause unexpected error.
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )

    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=__version__,
        help="Show program's version.",
    )

    parser.add_argument(
        "--cd",
        default=os.path.curdir,
        metavar="<path>",
        help="Change to directory before executing.",
        type=str,
    )

    # Sub commands
    subparsers = parser.add_subparsers(
        title="Available Commands",
        metavar="command",
        dest="cmd",
        help="Use `dvc command --help` for command-specific help.",
        required=True,
    )

    for cmd in COMMANDS:
        cmd.add_parser(subparsers, parent_parser)

    return parser
