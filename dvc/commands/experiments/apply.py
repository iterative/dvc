import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.commands import completion

logger = logging.getLogger(__name__)


class CmdExperimentsApply(CmdBase):
    def run(self):

        self.repo.experiments.apply(
            self.args.experiment, force=self.args.force
        )

        return 0


def add_parser(experiments_subparsers, parent_parser):

    EXPERIMENTS_APPLY_HELP = (
        "Apply the changes from an experiment to your workspace."
    )
    experiments_apply_parser = experiments_subparsers.add_parser(
        "apply",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_APPLY_HELP, "exp/apply"),
        help=EXPERIMENTS_APPLY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_apply_parser.add_argument(
        "--no-force",
        action="store_false",
        dest="force",
        help="Fail if this command would overwrite conflicting changes.",
    )
    experiments_apply_parser.add_argument(
        "experiment", help="Experiment to be applied."
    ).complete = completion.EXPERIMENT
    experiments_apply_parser.set_defaults(func=CmdExperimentsApply)
