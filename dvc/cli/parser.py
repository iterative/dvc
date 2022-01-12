"""Main parser for the dvc cli."""
import argparse
import logging
import os
import sys

from dvc.commands import (
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
    external,
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
    machine,
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

from . import DvcParserError

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
    machine,
]


def _find_parser(parser, cmd_cls):
    defaults = parser._defaults  # pylint: disable=protected-access
    if not cmd_cls or cmd_cls == defaults.get("func"):
        parser.print_help()
        raise DvcParserError

    actions = parser._actions  # pylint: disable=protected-access
    for action in actions:
        if not isinstance(action.choices, dict):
            # NOTE: we are only interested in subparsers
            continue
        for subparser in action.choices.values():
            _find_parser(subparser, cmd_cls)


class ArgumentErrorWithValue(argparse.ArgumentError):
    def __init__(self, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = value


class DvcParser(argparse.ArgumentParser):
    """Custom parser class for dvc CLI."""

    def error(self, message, cmd_cls=None):  # pylint: disable=arguments-differ
        logger.error(message)
        _find_parser(self, cmd_cls)

    def _check_value(self, action, value) -> None:
        try:
            super()._check_value(action, value)
        except argparse.ArgumentError as exc:
            # extending to include `value` so that we can access the subcommand
            # in `_parse_known_args`.
            raise ArgumentErrorWithValue(value, *exc.args) from exc

    def _parse_external_command(self, cmd, executable, arg_strings, namespace):
        namespace.cmd = cmd
        namespace.executable = executable
        # we pass `args` after the `cmd` to the `executable`
        # eg: `dvc -v ext hello` will be run as `dvc-ext hello`
        # i.e `-v` and similar global flags won't be passed down.
        # If they do support that, it needs to be specified after `cmd`
        # explicitly, i.e. `dvc -v ext hello -v`.
        cmd_pos = arg_strings.index(cmd)
        namespace.args = arg_strings[cmd_pos + 1 :]
        # emulating how we call `CmdBase` in `main`
        namespace.func = external.CmdExternal
        return namespace, []

    def _parse_known_args(self, arg_strings, namespace):
        try:
            return super()._parse_known_args(arg_strings, namespace)
        except ArgumentErrorWithValue as exc:
            # we only want to run this for subcommand
            if exc.argument_name != "COMMAND":
                raise

            import shutil

            executable = shutil.which(f"dvc-{exc.value}")
            if not executable:
                raise
            # NOTE: we still use `namespace` from parent parser, so that
            # all assumptions on `main()` still work. Below we add information
            # on how to run this, so that it could be executed later.
            return self._parse_external_command(
                exc.value, executable, arg_strings, namespace
            )

    def parse_args(self, args=None, namespace=None):
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
    from dvc._debug import add_debugging_flags

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

    from .utils import fix_subparsers

    fix_subparsers(subparsers)

    for cmd in COMMANDS:
        cmd.add_parser(subparsers, parent_parser)

    return parser
