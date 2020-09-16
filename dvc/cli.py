"""DVC command line interface"""
import argparse
import logging
import os
import sys
from difflib import get_close_matches

from .command import (
    add,
    cache,
    check_ignore,
    checkout,
    commit,
    completion,
    config,
    daemon,
    dag,
    data_sync,
    destroy,
    diff,
    experiments,
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
    dag,
    daemon,
    commit,
    completion,
    diff,
    version,
    update,
    git_hook,
    plots,
    experiments,
    check_ignore,
]


def _find_cmd_suggestions(cmd_arg, cmd_choices, cmd=None):
    """Find similar command suggestions for a typed command that contains typos.

    Args:
        cmd_arg: command argument typed in.
        cmd_choices: list of valid dvc commands to match against.

    Returns:
        String with command suggestions to display to the user if any exist.
    """
    if cmd:
        suggestion_str = (
            f"dvc: '{cmd} {cmd_arg}' is not a dvc command. See 'dvc --help'\n"
        )
    else:
        suggestion_str = (
            f"dvc: '{cmd_arg}' is not a dvc command. See 'dvc --help'\n"
        )
    suggestions = get_close_matches(cmd_arg, cmd_choices)
    if not suggestions:
        return suggestion_str

    if len(suggestions) > 1:
        suggestion_str += "\nThe most similar commands are"
    else:
        suggestion_str += "\nThe most similar command is"

    for suggestion in suggestions:
        if cmd:
            suggestion_str += f"\n\t{cmd} {suggestion}"
        else:
            suggestion_str += f"\n\t{suggestion}"

    return suggestion_str


def _find_parser(parser, cmd_cls):
    defaults = parser._defaults  # pylint: disable=protected-access
    if not cmd_cls or cmd_cls == defaults.get("func"):
        parser.print_help()
        raise DvcParserError()

    actions = parser._actions  # pylint: disable=protected-access
    for action in actions:
        if not isinstance(action.choices, dict):
            # NOTE: we are only interested in subparsers
            continue
        for subparser in action.choices.values():
            _find_parser(subparser, cmd_cls)


class DvcParser(argparse.ArgumentParser):
    """Custom parser class for dvc CLI."""

    cmd_choices = {}
    hidden_cmds = ["completion", "daemon", "exp", "experiments", "git-hook"]

    def error(self, message, cmd_cls=None):  # pylint: disable=arguments-differ
        logger.error(message)
        _find_parser(self, cmd_cls)

    def parse_args(self, args=None, namespace=None):
        # NOTE: this is a custom check to see if any suggestions can
        # be displayed to users in case a command contains typos
        # E.g. `dvc commti` would display
        # The most similar command is
        #         commit
        if args is None:
            args = sys.argv[1:]
        else:
            args = list(args)
        if (
            len(args) >= 1
            and args[0] not in self.cmd_choices
            and args[0] not in self.hidden_cmds
        ):
            cmd_suggestions = _find_cmd_suggestions(
                args[0], list(self.cmd_choices.keys())
            )
            logger.error(cmd_suggestions)
            raise DvcParserError

        # NOTE: this is a custom check to see if any suggestions can
        # be displayed to users in case a nested subcommand contains typos
        # E.g. `dvc remote modfiy` would display
        # The most similar command is
        #         remote modify
        if len(args) == 2 and args[0] in self.cmd_choices:
            sub_cmd_choices = self.cmd_choices[args[0]]
            if sub_cmd_choices and args[1] not in sub_cmd_choices:
                sub_cmd_suggestions = _find_cmd_suggestions(
                    args[1], sub_cmd_choices, args[0]
                )
                logger.error(sub_cmd_suggestions)
                raise DvcParserError

        # NOTE: overriding to provide a more granular help message.
        # E.g. `dvc plots diff --bad-flag` would result in a `dvc plots diff`
        # help message instead of generic `dvc` usage.
        args, argv = self.parse_known_args(args, namespace)
        if argv:
            msg = "unrecognized arguments: %s"
            self.error(msg % " ".join(argv), getattr(args, "func", None))
        return args


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

    parent_parser.add_argument(
        "--cprofile",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    parent_parser.add_argument("--cprofile-dump", help=argparse.SUPPRESS)

    parent_parser.add_argument(
        "--pdb", action="store_true", default=False, help=argparse.SUPPRESS,
    )

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
    help_action = parser.add_argument(
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
        metavar="COMMAND",
        dest="cmd",
        help="Use `dvc COMMAND --help` for command-specific help.",
    )

    fix_subparsers(subparsers)

    for cmd in COMMANDS:
        cmd.add_parser(subparsers, parent_parser)

    for cmd, subparser in subparsers.choices.items():
        if cmd in parser.hidden_cmds:
            continue
        parser.cmd_choices[cmd] = []
        actions = subparser._actions  # pylint: disable=protected-access
        for action in actions:
            if not isinstance(action.choices, dict):
                # NOTE: we are only interested in subparsers
                continue
            parser.cmd_choices[cmd].extend(action.choices.keys())

    # treat -h, --help as command choices
    for option in help_action.option_strings:
        parser.cmd_choices[option] = []

    return parser


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
