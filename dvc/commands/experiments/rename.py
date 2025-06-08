from dvc.cli import formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import InvalidArgumentError
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


class CmdExperimentsRename(CmdBase):
    def run(self):
        from dvc.utils import humanize

        if not (self.args.experiment and self.args.name):
            raise InvalidArgumentError(
                "An experiment to rename and a new experiment name are required."
            )
        renamed = self.repo.experiments.rename(
            exp_name=self.args.experiment,
            new_name=self.args.name,
            git_remote=self.args.git_remote,
            force=self.args.force,
        )
        if renamed:
            ui.write(f"Renamed experiments: {humanize.join(map(repr, renamed))}")
        else:
            ui.write("No experiments to rename.")

        return 0


def add_parser(experiments_subparsers, parent_parser):
    EXPERIMENTS_RENAME_HELP = "Rename experiments."
    experiments_rename_parser = experiments_subparsers.add_parser(
        "rename",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_RENAME_HELP, "exp/rename"),
        help=EXPERIMENTS_RENAME_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    rename_group = experiments_rename_parser.add_mutually_exclusive_group()
    rename_group.add_argument(
        "-g",
        "--git-remote",
        metavar="<git_remote>",
        help="Name or URL of the Git remote to rename the experiment from",
    )
    experiments_rename_parser.add_argument(
        "experiment",
        help="Experiment to rename.",
        nargs="?",
        metavar="<experiment>",
    )
    experiments_rename_parser.add_argument(
        "name",
        help="New name for the experiment.",
        metavar="<name>",
    )
    experiments_rename_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Replace experiment if it already exists.",
    )
    experiments_rename_parser.set_defaults(func=CmdExperimentsRename)
