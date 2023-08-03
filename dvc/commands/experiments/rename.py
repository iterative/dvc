import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import InvalidArgumentError
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdExperimentsRename(CmdBase):
    def check_arguments(self):
        if not any(
            [
                self.args.rev,
            ]
        ) ^ bool(self.args.experiment):
            raise InvalidArgumentError(
                "Either provide an `experiment` argument, or use the "
                "`--rev` or `--all-commits` or `--queue` flag."
            )

    def run(self):
        from dvc.utils import humanize

        self.check_arguments()

        renamed = self.repo.experiments.rename(
            exp_name=self.args.experiment,
            new_name=self.args.name,
            rev=self.args.rev,
            git_remote=self.args.git_remote,
        )
        if renamed:
            ui.write(f"Renamed experiments: {humanize.join(map(repr, renamed))}")
        else:
            ui.write("No experiments to rename.")

        return 0


def add_parser(experiments_subparsers, parent_parser):
    EXPERIMENTS_REMOVE_HELP = "Rename experiments."
    experiments_rename_parser = experiments_subparsers.add_parser(
        "rename",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_REMOVE_HELP, "exp/remove"),
        help=EXPERIMENTS_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remove_group = experiments_rename_parser.add_mutually_exclusive_group()
    remove_group.add_argument(
        "-g",
        "--git-remote",
        metavar="<git_remote>",
        help="Name or URL of the Git remote to rename the experiment from",
    )
    default_msg = " (HEAD by default)"
    msg = (
        f"Rename experiments derived from the specified `<commit>` as "
        f"baseline{default_msg}."
    )
    experiments_rename_parser.add_argument(
        "--rev",
        type=str,
        action="append",
        default=None,
        help=msg,
        metavar="<commit>",
    )
    experiments_rename_parser.add_argument(
        "experiment",
        help="Experiment to rename.",
        nargs="?",
        metavar="<experiment>",
    )
    experiments_rename_parser.add_argument(
        "name",
        help="Name of new experiment.",
        metavar="<name>",
    )
    experiments_rename_parser.set_defaults(func=CmdExperimentsRename)
