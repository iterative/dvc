from dvc.cli import formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


class CmdExperimentsPull(CmdBase):
    def run(self):
        pulled_exps = self.repo.experiments.pull(
            self.args.git_remote,
            self.args.experiment,
            all_commits=self.args.all_commits,
            rev=self.args.rev,
            num=self.args.num,
            force=self.args.force,
            pull_cache=self.args.pull_cache,
            dvc_remote=self.args.dvc_remote,
            jobs=self.args.jobs,
            run_cache=self.args.run_cache,
        )

        if pulled_exps:
            ui.write(
                f"Pulled experiment '{pulled_exps}'",
                f"from Git remote '{self.args.git_remote}'.",
            )
        else:
            ui.write("No experiments to pull.")
        if not self.args.pull_cache:
            ui.write(
                "To pull cached outputs for this experiment"
                "from DVC remote storage,"
                "re-run this command without '--no-cache'."
            )

        return 0


def add_parser(experiments_subparsers, parent_parser):
    from . import add_rev_selection_flags

    EXPERIMENTS_PULL_HELP = "Pull an experiment from a Git remote."
    experiments_pull_parser = experiments_subparsers.add_parser(
        "pull",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_PULL_HELP, "exp/pull"),
        help=EXPERIMENTS_PULL_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    add_rev_selection_flags(experiments_pull_parser, "Pull", True)
    experiments_pull_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Replace local experiment if it already exists.",
    )
    experiments_pull_parser.add_argument(
        "--no-cache",
        action="store_false",
        dest="pull_cache",
        help="Do not pull cached outputs for this experiment from DVC remote storage.",
    )
    experiments_pull_parser.add_argument(
        "-r",
        "--remote",
        dest="dvc_remote",
        metavar="<name>",
        help="Name of the DVC remote to use when pulling cached outputs.",
    )
    experiments_pull_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        metavar="<number>",
        help=(
            "Number of jobs to run simultaneously when pulling from DVC remote storage."
        ),
    )
    experiments_pull_parser.add_argument(
        "--run-cache",
        action="store_true",
        default=False,
        help="Pull run history for all stages.",
    )
    experiments_pull_parser.add_argument(
        "git_remote",
        help="Git remote name or Git URL.",
        metavar="<git_remote>",
    )
    experiments_pull_parser.add_argument(
        "experiment",
        nargs="*",
        default=None,
        help="Experiments to pull.",
        metavar="<experiment>",
    )
    experiments_pull_parser.set_defaults(func=CmdExperimentsPull)
