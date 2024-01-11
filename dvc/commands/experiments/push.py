from typing import Any, Dict

from dvc.cli import completion, formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


class CmdExperimentsPush(CmdBase):
    @staticmethod
    def log_result(result: Dict[str, Any], remote: str):
        from dvc.utils import humanize

        def join_exps(exps):
            return humanize.join([f"[bold]{e}[/]" for e in exps])

        if diverged_exps := result.get("diverged"):
            exps = join_exps(diverged_exps)
            ui.error_write(
                f"[yellow]Local experiment {exps} has diverged "
                "from remote experiment with the same name.\n"
                "To override the remote experiment re-run with '--force'.",
                styled=True,
            )
        if uptodate_exps := result.get("up_to_date"):
            exps = join_exps(uptodate_exps)
            verb = "are" if len(uptodate_exps) > 1 else "is"
            ui.write(
                f"Experiment {exps} {verb} up to date on Git remote {remote!r}.",
                styled=True,
            )
        if pushed_exps := result.get("success"):
            exps = join_exps(pushed_exps)
            ui.write(f"Pushed experiment {exps} to Git remote {remote!r}.", styled=True)
        if not uptodate_exps and not pushed_exps:
            ui.write("No experiments to push.")

        if uploaded := result.get("uploaded"):
            stats = {"uploaded": uploaded}
            ui.write(humanize.get_summary(stats.items()))

        if project_url := result.get("url"):
            ui.rich_print(
                "View your experiments at", project_url, style="yellow", soft_wrap=True
            )

    def run(self):
        from dvc.repo.experiments.push import UploadError

        try:
            result = self.repo.experiments.push(
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
        except UploadError as e:
            self.log_result(e.result, self.args.git_remote)
            raise

        self.log_result(result, self.args.git_remote)
        if not self.args.push_cache:
            ui.write(
                "To push cached outputs",
                (
                    "for this experiment to DVC remote storage,"
                    "re-run this command without '--no-cache'."
                ),
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
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    add_rev_selection_flags(experiments_push_parser, "Push", True)
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
        help="Do not push cached outputs for this experiment to DVC remote storage.",
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
        help="Number of jobs to run simultaneously when pushing to DVC remote storage.",
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
