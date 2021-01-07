"""DVC command line interface"""
import argparse
import logging
import os
import sys
import types
from difflib import get_close_matches

from ._debug import add_debugging_flags
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
    live,
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
    stage,
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
    stage,
    experiments,
    check_ignore,
    live,
]


def _get_cmd_suggestion_msg(cmd_arg, cmd_choices, cmd=None):
    """Find similar command suggestions for a typed command that contains typos.

    Args:
        cmd_arg: command argument typed in.
        cmd_choices: list of valid dvc commands to match against.

    Returns:
        String with command suggestions to display to the user if any exist.
    """
    base_suggestion = "is not a dvc command. See 'dvc --help'\n"
    if cmd:
        suggestion_str = f"dvc: '{cmd} {cmd_arg}' {base_suggestion}"
    else:
        suggestion_str = f"dvc: '{cmd_arg}' {base_suggestion}"
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

    def error(self, message, cmd_cls=None):  # pylint: disable=arguments-differ
        logger.error(message)
        _find_parser(self, cmd_cls)

    def parse_args(self, args=None, namespace=None):
        # NOTE: overriding to provide a more granular help message.
        # E.g. `dvc plots diff --bad-flag` would result in a `dvc plots diff`
        # help message instead of generic `dvc` usage.
        try:
            args, argv = self.parse_known_args(args, namespace)
        except argparse.ArgumentError:
            if args is None:
                args = sys.argv[1:]
            else:
                args = list(args)
            self._find_cmd_suggestions(args)
        else:
            if argv:
                msg = "unrecognized arguments: %s"
                self.error(msg % " ".join(argv), getattr(args, "func", None))
            return args

    def _get_cmd_choices(self):
        """Keep track of public dvc commands and hidden dvc commands.

        Returns:
            A dictionary with public dvc commands mapped to a list containing
            dvc subcommands (if they exist).

            A list containing hidden dvc commands to be ignored when finding
            suggestions.
        """
        cmd_choices = {}
        hidden_cmds = []

        parser_actions = self._actions  # pylint: disable=protected-access
        for parser_action in parser_actions:
            if parser_action.dest == "help":
                # treat -h, --help as command choices
                for option in parser_action.option_strings:
                    cmd_choices[option] = []
            elif parser_action.dest == "cmd":
                for cmd, subparser in parser_action.choices.items():
                    if not subparser.add_help:
                        hidden_cmds.append(cmd)
                        continue
                    cmd_choices[cmd] = []
                    actions = (
                        subparser._actions  # pylint: disable=protected-access
                    )
                    for action in actions:
                        if not isinstance(action.choices, dict):
                            # NOTE: we are only interested in subcommands
                            continue
                        cmd_choices[cmd].extend(action.choices.keys())

        return cmd_choices, hidden_cmds

    def _find_cmd_suggestions(self, args):
        """Find similar command suggestions for commands and subcommands
        that contain typos. Show suggestions for public commands, do not show
        suggestions for hidden commands.

        Args:
            args: argument strings.

        Raises:
            dvc.exceptions.DvcParserError: raised for found suggestions.
        """
        cmd_choices, hidden_cmds = self._get_cmd_choices()

        # NOTE: Check top level dvc commands for suggestions
        # E.g. `dvc commti` would display
        # The most similar command is
        #         commit
        if (
            len(args) >= 1
            and args[0] not in cmd_choices
            and args[0] not in hidden_cmds
        ):
            cmd_suggestion_msg = _get_cmd_suggestion_msg(
                args[0], list(cmd_choices.keys())
            )
            logger.error(cmd_suggestion_msg)
            raise DvcParserError

        # NOTE: Check nested dvc subcommands for suggestions
        # E.g. `dvc remote modfiy` would display
        # The most similar command is
        #         remote modify
        if len(args) == 2 and args[0] in cmd_choices:
            sub_cmd_choices = cmd_choices[args[0]]
            if sub_cmd_choices and args[1] not in sub_cmd_choices:
                cmd_suggestion_msg = _get_cmd_suggestion_msg(
                    args[1], sub_cmd_choices, args[0]
                )
                logger.error(cmd_suggestion_msg)
                raise DvcParserError


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
    add_debugging_flags(parent_parser)
    log_level_group = parent_parser.add_mutually_exclusive_group()
    log_level_group.add_argument(
        "-q", "--quiet", action="count", default=0, help="Be quiet."
    )
    log_level_group.add_argument(
        "-v", "--verbose", action="count", default=0, help="Be verbose."
    )

    return parent_parser


def add_parser_exit_on_error(self, name, **kwargs):
    """Workaround for dynamically setting exit_on_error for subparsers on
    Python 3.9. See more info at:
        https://github.com/python/cpython/blob/master/Lib/argparse.py
    """
    if sys.version_info >= (3, 9, 0) and name not in ["config"]:
        kwargs["exit_on_error"] = False

    # set prog from the existing prefix
    if kwargs.get("prog") is None:
        kwargs["prog"] = "%s %s" % (
            self._prog_prefix,  # pylint: disable=protected-access
            name,
        )  # pylint: disable=protected-access

    aliases = kwargs.pop("aliases", ())

    # create a pseudo-action to hold the choice help
    if "help" in kwargs:
        help = kwargs.pop("help")  # pylint: disable=redefined-builtin
        choice_action = self._ChoicesPseudoAction(
            name, aliases, help
        )  # pylint: disable=protected-access
        self._choices_actions.append(
            choice_action
        )  # pylint: disable=protected-access

    # create the parser and add it to the map
    parser = self._parser_class(**kwargs)
    self._name_parser_map[name] = parser  # pylint: disable=protected-access

    # make parser available under aliases also
    for alias in aliases:
        self._name_parser_map[
            alias
        ] = parser  # pylint: disable=protected-access

    return parser


def get_main_parser():
    parent_parser = get_parent_parser()

    # Main parser
    desc = "Data Version Control"
    if sys.version_info >= (3, 9, 0):
        parser = DvcParser(
            prog="dvc",
            description=desc,
            parents=[parent_parser],
            formatter_class=argparse.RawTextHelpFormatter,
            add_help=False,
            exit_on_error=False,  # pylint: disable=unexpected-keyword-arg
        )
    else:
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

    subparsers.add_parser = types.MethodType(
        add_parser_exit_on_error, subparsers
    )

    for cmd in COMMANDS:
        cmd.add_parser(subparsers, parent_parser)

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
