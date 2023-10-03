import argparse
import logging

from dvc.cli import completion
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link

logger = logging.getLogger(__name__)


class CmdExperimentsApply(CmdBase):
    def run(self):
        self.repo.experiments.apply(self.args.experiment, force=self.args.force)

        return 0


def add_parser(experiments_subparsers, parent_parser):
    EXPERIMENTS_APPLY_HELP = "Apply the changes from an experiment to your workspace."
    experiments_apply_parser = experiments_subparsers.add_parser(
        "apply",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_APPLY_HELP, "exp/apply"),
        help=EXPERIMENTS_APPLY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_apply_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Do not prompt when removing working directory files.",
    )
    experiments_apply_parser.add_argument(
        "experiment", help="Experiment to be applied."
    ).complete = completion.EXPERIMENT
    experiments_apply_parser.set_defaults(func=CmdExperimentsApply)
