import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import InvalidArgumentError
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdExperimentsRemove(CmdBase):
    def raise_error_if_all_disabled(self):
        if not any(
            [
                self.args.experiment,
                self.args.all_commits,
                self.args.rev,
                self.args.queue,
            ]
        ):
            raise InvalidArgumentError(
                "Either provide an `experiment` argument, or use the "
                "`--rev` or `--all-commits` flag."
            )

    def run(self):

        self.raise_error_if_all_disabled()

        removed_list = self.repo.experiments.remove(
            exp_names=self.args.experiment,
            all_commits=self.args.all_commits,
            rev=self.args.rev,
            num=self.args.num,
            queue=self.args.queue,
            git_remote=self.args.git_remote,
        )
        removed = ",".join(removed_list)
        ui.write(f"Removed experiments: {removed}")

        return 0


def add_parser(experiments_subparsers, parent_parser):
    from . import add_rev_selection_flags

    EXPERIMENTS_REMOVE_HELP = "Remove experiments."
    experiments_remove_parser = experiments_subparsers.add_parser(
        "remove",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_REMOVE_HELP, "exp/remove"),
        help=EXPERIMENTS_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remove_group = experiments_remove_parser.add_mutually_exclusive_group()
    add_rev_selection_flags(experiments_remove_parser, "Remove", False)
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
