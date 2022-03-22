import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.commands import completion
from dvc.exceptions import InvalidArgumentError
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdExperimentsPush(CmdBase):
    def raise_error_if_all_disabled(self):
        if not any(
            [self.args.experiment, self.args.all_commits, self.args.rev]
        ):
            raise InvalidArgumentError(
                "Either provide an `experiment` argument, or use the "
                "`--rev` or `--all-commits` flag."
            )

    def run(self):

        self.raise_error_if_all_disabled()

        pushed_exps = self.repo.experiments.push(
            self.args.git_remote,
            self.args.experiment,
            all_commits=self.args.all_commits,
            rev=self.args.rev,
            num=self.args.num,
            force=self.args.force,
            push_cache=self.args.push_cache,
            dvc_remote=self.args.dvc_remote,
            jobs=self.args.jobs,
            run_cache=self.args.run_cache,
        )

        if pushed_exps:
            ui.write(
                f"Pushed experiment '{pushed_exps}'"
                f"to Git remote '{self.args.git_remote}'."
            )
        else:
            ui.write("No experiments to push.")
        if not self.args.push_cache:
            ui.write(
                "To push cached outputs",
                "for this experiment to DVC remote storage,"
                "re-run this command without '--no-cache'.",
            )

        return 0


def add_parser(experiments_subparsers, parent_parser):
    from . import add_rev_selection_flags

    EXPERIMENTS_PUSH_HELP = "Push a local experiment to a Git remote."
    experiments_push_parser = experiments_subparsers.add_parser(
        "push",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_PUSH_HELP, "exp/push"),
        help=EXPERIMENTS_PUSH_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_rev_selection_flags(experiments_push_parser, "Push", False)
    experiments_push_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Replace experiment in the Git remote if it already exists.",
    )
    experiments_push_parser.add_argument(
        "--no-cache",
        action="store_false",
        dest="push_cache",
        help=(
            "Do not push cached outputs for this experiment to DVC remote "
            "storage."
        ),
    )
    experiments_push_parser.add_argument(
        "-r",
        "--remote",
        dest="dvc_remote",
        metavar="<name>",
        help="Name of the DVC remote to use when pushing cached outputs.",
    )
    experiments_push_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        metavar="<number>",
        help=(
            "Number of jobs to run simultaneously when pushing to DVC remote "
            "storage."
        ),
    )
    experiments_push_parser.add_argument(
        "--run-cache",
        action="store_true",
        default=False,
        help="Push run history for all stages.",
    )
    experiments_push_parser.add_argument(
        "git_remote",
        help="Git remote name or Git URL.",
        metavar="<git_remote>",
    )
    experiments_push_parser.add_argument(
        "experiment",
        nargs="*",
        default=None,
        help="Experiments to push.",
        metavar="<experiment>",
    ).complete = completion.EXPERIMENT
    experiments_push_parser.set_defaults(func=CmdExperimentsPush)
