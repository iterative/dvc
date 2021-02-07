import argparse
import logging

from dvc.command.base import CmdBase, append_doc_link
from dvc.command.stage import parse_cmd
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
                self.args.cmd,
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
                cmd=parse_cmd(self.args.cmd),
                outs=self.args.outs,
                outs_no_cache=self.args.outs_no_cache,
                metrics=self.args.metrics,
                metrics_no_cache=self.args.metrics_no_cache,
                plots=self.args.plots,
                plots_no_cache=self.args.plots_no_cache,
                live=self.args.live,
                live_no_cache=self.args.live_no_cache,
                live_no_summary=self.args.live_no_summary,
                live_no_html=self.args.live_no_html,
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
                desc=self.args.desc,
            )
        except DvcException:
            logger.exception("")
            return 1

        return 0


def add_parser(subparsers, parent_parser):
    from dvc.command.stage import _add_common_args

    RUN_HELP = (
        "Generate a dvc.yaml file from a command and execute the command."
    )
    run_parser = subparsers.add_parser(
        "run",
        parents=[parent_parser],
        description=append_doc_link(RUN_HELP, "run"),
        help=RUN_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    run_parser.add_argument(
        "-n", "--name", help="Stage name.",
    )
    run_parser.add_argument(
        "--file", metavar="<filename>", help=argparse.SUPPRESS,
    )
    run_parser.add_argument(
        "--no-exec",
        action="store_true",
        default=False,
        help="Only create dvc.yaml without actually running it.",
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
        "--single-stage",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    _add_common_args(run_parser)
    run_parser.set_defaults(func=CmdRun)
