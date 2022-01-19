import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link

logger = logging.getLogger(__name__)


class CmdExperimentsBranch(CmdBase):
    def run(self):

        self.repo.experiments.branch(self.args.experiment, self.args.branch)

        return 0


def add_parser(experiments_subparsers, parent_parser):

    EXPERIMENTS_BRANCH_HELP = "Promote an experiment to a Git branch."
    experiments_branch_parser = experiments_subparsers.add_parser(
        "branch",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_BRANCH_HELP, "exp/branch"),
        help=EXPERIMENTS_BRANCH_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_branch_parser.add_argument(
        "experiment", help="Experiment to be promoted."
    )
    experiments_branch_parser.add_argument(
        "branch", help="Git branch name to use."
    )
    experiments_branch_parser.set_defaults(func=CmdExperimentsBranch)
