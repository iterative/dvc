"""DVC command line interface"""
import argparse
import logging
import sys

from .command import (
    add,
    cache,
    checkout,
    commit,
    config,
    daemon,
    data_sync,
    destroy,
    diff,
    freeze,
    gc,
    get,
    get_url,
    git_hook,
    imp,
    imp_url,
    init,
    install,
    ls,
    metrics,
    move,
    params,
    pipeline,
    plots,
    remote,
    remove,
    repro,
    root,
    run,
    unprotect,
    update,
    version,
)
from .command.base import fix_subparsers
from .exceptions import DvcParserError

logger = logging.getLogger(__name__)

COMMANDS = [
    init,
    get,
    get_url,
    destroy,
    add,
    remove,
    move,
    unprotect,
    run,
    repro,
    data_sync,
    gc,
    imp,
    imp_url,
    config,
    checkout,
    remote,
    cache,
    metrics,
    params,
    install,
    root,
    ls,
    freeze,
    pipeline,
    daemon,
    commit,
    diff,
    version,
    update,
    git_hook,
    plots,
]


class DvcParser(argparse.ArgumentParser):
    """Custom parser class for dvc CLI."""

    def error(self, message):
        """Custom error method.
        Args:
            message (str): error message.

        Raises:
            dvc.exceptions.DvcParser: dvc parser exception.
        """
        logger.error(message)
        self.print_help()
        raise DvcParserError()


class VersionAction(argparse.Action):  # pragma: no cover
    # pylint: disable=too-few-public-methods
    """Shows DVC version and exits."""

    def __call__(self, parser, namespace, values, option_string=None):
        from dvc import __version__

        print(__version__)
        sys.exit(0)


def get_parent_parser():
    """Create instances of a parser containing common arguments shared among
    all the commands.

    When overwriting `-q` or `-v`, you need to instantiate a new object
    in order to prevent some weird behavior.
    """
    parent_parser = argparse.ArgumentParser(add_help=False)

    log_level_group = parent_parser.add_mutually_exclusive_group()
    log_level_group.add_argument(
        "-q", "--quiet", action="count", default=0, help="Be quiet."
    )
    log_level_group.add_argument(
        "-v", "--verbose", action="count", default=0, help="Be verbose."
    )

    return parent_parser


def get_main_parser():
    parent_parser = get_parent_parser()

    # Main parser
    desc = "Data Version Control"
    parser = DvcParser(
        prog="dvc",
        description=desc,
        parents=[parent_parser],
        formatter_class=argparse.RawTextHelpFormatter,
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

    # NOTE: On some python versions action='version' prints to stderr
    # instead of stdout https://bugs.python.org/issue18920
    parser.add_argument(
        "-V",
        "--version",
        action=VersionAction,
        nargs=0,
        help="Show program's version.",
    )

    # Sub commands
    subparsers = parser.add_subparsers(
        title="Available Commands",
        metavar="COMMAND",
        dest="cmd",
        help="Use `dvc COMMAND --help` for command-specific help.",
    )

    fix_subparsers(subparsers)

    for cmd in COMMANDS:
        cmd.add_parser(subparsers, parent_parser)

    return parser


def print_parser(parser, level=0):
    """
    >>> print_parser(get_main_parser())
    """
    opts = sum(
        (opt.option_strings for opt in parser._get_optional_actions()), []
    )
    print(" " * level, " ".join(opts))
    for sub in parser._get_positional_actions():
        if sub.choices:
            for opt, child in sub.choices.items():
                print(" " * level, opt)
                print_parser(child, level + 1)
        else:
            print(" " * level, "<" + sub.dest + ">")


def parse_args(argv=None):
    """Parses CLI arguments.

    Args:
        argv: optional list of arguments to parse. sys.argv is used by default.

    Raises:
        dvc.exceptions.DvcParserError: raised for argument parsing errors.
    """
    parser = get_main_parser()
    args = parser.parse_args(argv)
    return args
