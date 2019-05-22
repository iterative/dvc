from __future__ import unicode_literals

import argparse
import logging

from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


class CmdRun(CmdBase):
    def run(self):
        overwrite = self.args.yes or self.args.overwrite_dvcfile

        if not any(
            [
                self.args.deps,
                self.args.outs,
                self.args.outs_no_cache,
                self.args.metrics,
                self.args.metrics_no_cache,
                self.args.outs_persist,
                self.args.outs_persist_no_cache,
                self.args.command,
            ]
        ):  # pragma: no cover
            logger.error(
                "too few arguments. Specify at least one: '-d', '-o', '-O',"
                " '-m', '-M', '--outs-persist', '--outs-persist-no-cache',"
                " 'command'."
            )
            return 1

        try:
            self.repo.run(
                cmd=self._parsed_cmd(),
                outs=self.args.outs,
                outs_no_cache=self.args.outs_no_cache,
                metrics=self.args.metrics,
                metrics_no_cache=self.args.metrics_no_cache,
                deps=self.args.deps,
                fname=self.args.file,
                cwd=self.args.cwd,
                wdir=self.args.wdir,
                no_exec=self.args.no_exec,
                overwrite=overwrite,
                ignore_build_cache=self.args.ignore_build_cache,
                remove_outs=self.args.remove_outs,
                no_commit=self.args.no_commit,
                outs_persist=self.args.outs_persist,
                outs_persist_no_cache=self.args.outs_persist_no_cache,
            )
        except DvcException:
            logger.exception("failed to run command")
            return 1

        return 0

    def _parsed_cmd(self):
        """
        We need to take into account two cases:

        - ['python code.py foo bar']: Used mainly with dvc as a library
        - ['echo', 'foo bar']: List of arguments received from the CLI

        The second case would need quoting, as it was passed through:
                dvc run echo "foo bar"
        """
        if len(self.args.command) < 2:
            return " ".join(self.args.command)

        return " ".join(self._quote_argument(arg) for arg in self.args.command)

    def _quote_argument(self, argument):
        if " " not in argument or '"' in argument:
            return argument

        return '"{}"'.format(argument)


def add_parser(subparsers, parent_parser):
    RUN_HELP = "Generate a stage file from a command and execute the command."
    run_parser = subparsers.add_parser(
        "run",
        parents=[parent_parser],
        description=append_doc_link(RUN_HELP, "run"),
        help=RUN_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    run_parser.add_argument(
        "-d",
        "--deps",
        action="append",
        default=[],
        help="Declare dependencies for reproducible cmd.",
    )
    run_parser.add_argument(
        "-o",
        "--outs",
        action="append",
        default=[],
        help="Declare output file or directory.",
    )
    run_parser.add_argument(
        "-O",
        "--outs-no-cache",
        action="append",
        default=[],
        help="Declare output file or directory "
        "(do not put into DVC cache).",
    )
    run_parser.add_argument(
        "-m",
        "--metrics",
        action="append",
        default=[],
        help="Declare output metric file or directory.",
    )
    run_parser.add_argument(
        "-M",
        "--metrics-no-cache",
        action="append",
        default=[],
        help="Declare output metric file or directory "
        "(do not put into DVC cache).",
    )
    run_parser.add_argument(
        "-f", "--file", help="Specify name of the DVC file it generates."
    )
    run_parser.add_argument(
        "-c", "--cwd", default=None, help="Deprecated, use -w and -f instead."
    )
    run_parser.add_argument(
        "-w",
        "--wdir",
        default=None,
        help="Directory within your repo to run your command in.",
    )
    run_parser.add_argument(
        "--no-exec",
        action="store_true",
        default=False,
        help="Only create stage file without actually running it.",
    )
    run_parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        default=False,
        help="Deprecated, use --overwrite-dvcfile instead",
    )
    run_parser.add_argument(
        "--overwrite-dvcfile",
        action="store_true",
        default=False,
        help="Overwrite existing dvc file without asking for confirmation.",
    )
    run_parser.add_argument(
        "--ignore-build-cache",
        action="store_true",
        default=False,
        help="Run this stage even if it has been already ran with the same "
        "command/dependencies/outputs/etc before.",
    )
    run_parser.add_argument(
        "--remove-outs",
        action="store_true",
        default=False,
        help="Deprecated, this is now the default behavior",
    )
    run_parser.add_argument(
        "--no-commit",
        action="store_true",
        default=False,
        help="Don't put files/directories into cache.",
    )
    run_parser.add_argument(
        "--outs-persist",
        action="append",
        default=[],
        help="Declare output file or directory that will not be "
        "removed upon repro.",
    )
    run_parser.add_argument(
        "--outs-persist-no-cache",
        action="append",
        default=[],
        help="Declare output file or directory that will not be "
        "removed upon repro (do not put into DVC cache).",
    )
    run_parser.add_argument(
        "command", nargs=argparse.REMAINDER, help="Command to execute."
    )
    run_parser.set_defaults(func=CmdRun)
