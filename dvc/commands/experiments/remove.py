from dvc.cli import formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import InvalidArgumentError
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


class CmdExperimentsRemove(CmdBase):
    def check_arguments(self):
        if not any(
            [
                self.args.all_commits,
                self.args.rev,
                self.args.queue,
            ]
        ) ^ bool(self.args.experiment):
            raise InvalidArgumentError(
                "Either provide an `experiment` argument, or use the "
                "`--rev` or `--all-commits` or `--queue` flag."
            )

    def run(self):
        from dvc.utils import humanize

        self.check_arguments()

        removed = self.repo.experiments.remove(
            exp_names=self.args.experiment,
            all_commits=self.args.all_commits,
            rev=self.args.rev,
            num=self.args.num,
            queue=self.args.queue,
            git_remote=self.args.git_remote,
            keep=self.args.keep,
        )
        if removed:
            ui.write(f"Removed experiments: {humanize.join(map(repr, removed))}")
        else:
            ui.write("No experiments to remove.")

        return 0


def add_parser(experiments_subparsers, parent_parser):
    from . import add_keep_selection_flag, add_rev_selection_flags

    EXPERIMENTS_REMOVE_HELP = "Remove experiments."
    experiments_remove_parser = experiments_subparsers.add_parser(
        "remove",
        aliases=["rm"],
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_REMOVE_HELP, "exp/remove"),
        help=EXPERIMENTS_REMOVE_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    remove_group = experiments_remove_parser.add_mutually_exclusive_group()
    add_rev_selection_flags(experiments_remove_parser, "Remove", False)
    add_keep_selection_flag(experiments_remove_parser)
    remove_group.add_argument(
        "--queue", action="store_true", help="Remove all queued experiments."
    )
    remove_group.add_argument(
        "-g",
        "--git-remote",
        metavar="<git_remote>",
        help="Name or URL of the Git remote to remove the experiment from",
    )
    experiments_remove_parser.add_argument(
        "experiment",
        nargs="*",
        help="Experiments to remove.",
        metavar="<experiment>",
    )
    experiments_remove_parser.set_defaults(func=CmdExperimentsRemove)
