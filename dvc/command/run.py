import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdRun(CmdBase):
    def run(self):
        if not any(
            [
                self.args.deps,
                self.args.outs,
                self.args.outs_no_cache,
                self.args.metrics,
                self.args.metrics_no_cache,
                self.args.plots,
                self.args.plots_no_cache,
                self.args.outs_persist,
                self.args.outs_persist_no_cache,
                self.args.checkpoints,
                self.args.params,
                self.args.command,
            ]
        ):  # pragma: no cover
            logger.error(
                "too few arguments. Specify at least one: `-d`, `-o`, `-O`, "
                "`-m`, `-M`, `-p`, `--plots`, `--plots-no-cache`, "
                "`--outs-persist`, `--outs-persist-no-cache`, `command`."
            )
            return 1

        try:
            self.repo.run(
                cmd=self._parsed_cmd(),
                outs=self.args.outs,
                outs_no_cache=self.args.outs_no_cache,
                metrics=self.args.metrics,
                metrics_no_cache=self.args.metrics_no_cache,
                plots=self.args.plots,
                plots_no_cache=self.args.plots_no_cache,
                deps=self.args.deps,
                params=self.args.params,
                fname=self.args.file,
                wdir=self.args.wdir,
                no_exec=(self.args.no_exec or bool(self.args.checkpoints)),
                force=self.args.force,
                run_cache=not self.args.no_run_cache,
                no_commit=self.args.no_commit,
                outs_persist=self.args.outs_persist,
                outs_persist_no_cache=self.args.outs_persist_no_cache,
                checkpoints=self.args.checkpoints,
                always_changed=self.args.always_changed,
                name=self.args.name,
                single_stage=self.args.single_stage,
                external=self.args.external,
            )
        except DvcException:
            logger.exception("")
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

        return f'"{argument}"'


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
        metavar="<path>",
    ).complete = completion.FILE
    run_parser.add_argument(
        "-n", "--name", help="Stage name.",
    )
    run_parser.add_argument(
        "-o",
        "--outs",
        action="append",
        default=[],
        help="Declare output file or directory.",
        metavar="<filename>",
    ).complete = completion.FILE
    run_parser.add_argument(
        "-O",
        "--outs-no-cache",
        action="append",
        default=[],
        help="Declare output file or directory "
        "(do not put into DVC cache).",
        metavar="<filename>",
    ).complete = completion.FILE
    run_parser.add_argument(
        "-p",
        "--params",
        action="append",
        default=[],
        help="Declare parameter to use as additional dependency.",
        metavar="[<filename>:]<params_list>",
    ).complete = completion.FILE
    run_parser.add_argument(
        "-m",
        "--metrics",
        action="append",
        default=[],
        help="Declare output metrics file.",
        metavar="<path>",
    )
    run_parser.add_argument(
        "-M",
        "--metrics-no-cache",
        action="append",
        default=[],
        help="Declare output metrics file (do not put into DVC cache).",
        metavar="<path>",
    )
    run_parser.add_argument(
        "--plots",
        action="append",
        default=[],
        help="Declare output plot file.",
        metavar="<path>",
    )
    run_parser.add_argument(
        "--plots-no-cache",
        action="append",
        default=[],
        help="Declare output plot file (do not put into DVC cache).",
        metavar="<path>",
    )
    run_parser.add_argument(
        "--file", metavar="<filename>", help=argparse.SUPPRESS,
    )
    run_parser.add_argument(
        "-w",
        "--wdir",
        help="Directory within your repo to run your command in.",
        metavar="<path>",
    )
    run_parser.add_argument(
        "--no-exec",
        action="store_true",
        default=False,
        help="Only create stage file without actually running it.",
    )
    run_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Overwrite existing stage",
    )
    run_parser.add_argument(
        "--no-run-cache",
        action="store_true",
        default=False,
        help=(
            "Execute the command even if this stage has already been run "
            "with the same command/dependencies/outputs/etc before."
        ),
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
        metavar="<filename>",
    )
    run_parser.add_argument(
        "--outs-persist-no-cache",
        action="append",
        default=[],
        help="Declare output file or directory that will not be "
        "removed upon repro (do not put into DVC cache).",
        metavar="<filename>",
    )
    run_parser.add_argument(
        "-c",
        "--checkpoints",
        action="append",
        default=[],
        help=argparse.SUPPRESS,
        metavar="<filename>",
    ).complete = completion.FILE
    run_parser.add_argument(
        "--always-changed",
        action="store_true",
        default=False,
        help="Always consider this DVC-file as changed.",
    )
    run_parser.add_argument(
        "--single-stage",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    run_parser.add_argument(
        "--external",
        action="store_true",
        default=False,
        help="Allow outputs that are outside of the DVC repository.",
    )
    run_parser.add_argument(
        "command", nargs=argparse.REMAINDER, help="Command to execute."
    )
    run_parser.set_defaults(func=CmdRun)
