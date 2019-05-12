"""Command line interface for dvc."""

from __future__ import print_function
from __future__ import unicode_literals

import sys
import argparse
import logging

from dvc.command.base import fix_subparsers
import dvc.command.init as init
import dvc.command.pkg as pkg
import dvc.command.destroy as destroy
import dvc.command.remove as remove
import dvc.command.move as move
import dvc.command.unprotect as unprotect
import dvc.command.run as run
import dvc.command.repro as repro
import dvc.command.data_sync as data_sync
import dvc.command.gc as gc
import dvc.command.add as add
import dvc.command.imp as imp
import dvc.command.config as config
import dvc.command.checkout as checkout
import dvc.command.remote as remote
import dvc.command.cache as cache
import dvc.command.metrics as metrics
import dvc.command.install as install
import dvc.command.root as root
import dvc.command.lock as lock
import dvc.command.pipeline as pipeline
import dvc.command.daemon as daemon
import dvc.command.commit as commit
import dvc.command.tag as tag
import dvc.command.diff as diff
import dvc.command.version as version
from dvc.exceptions import DvcParserError


logger = logging.getLogger(__name__)

COMMANDS = [
    init,
    pkg,
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
    config,
    checkout,
    remote,
    cache,
    metrics,
    install,
    root,
    lock,
    pipeline,
    daemon,
    commit,
    tag,
    diff,
    version,
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
    """Shows dvc version and exits."""

    def __call__(self, parser, namespace, values, option_string=None):
        from dvc import __version__

        print(__version__)
        sys.exit(0)


def get_parent_parser():
    """Create instances of a parser containing common arguments shared among
    all the commands.

    When overwritting `-q` or `-v`, you need to instantiate a new object
    in order to prevent some weird behavior.
    """
    parent_parser = argparse.ArgumentParser(add_help=False)

    log_level_group = parent_parser.add_mutually_exclusive_group()
    log_level_group.add_argument(
        "-q", "--quiet", action="store_true", default=False, help="Be quiet."
    )
    log_level_group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Be verbose.",
    )

    return parent_parser


def parse_args(argv=None):
    """Parses CLI arguments.

    Args:
        argv: optional list of arguments to parse. sys.argv is used by default.

    Raises:
        dvc.exceptions.DvcParserError: raised for argument parsing errors.
    """
    parent_parser = get_parent_parser()

    # Main parser
    desc = "Data Version Control"
    parser = DvcParser(
        prog="dvc",
        description=desc,
        parents=[parent_parser],
        formatter_class=argparse.RawTextHelpFormatter,
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
        help="Use dvc COMMAND --help for command-specific help.",
    )

    fix_subparsers(subparsers)

    for cmd in COMMANDS:
        cmd.add_parser(subparsers, parent_parser)

    args = parser.parse_args(argv)

    return args
