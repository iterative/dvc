import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link

logger = logging.getLogger(__name__)


class CmdExperimentsClean(CmdBase):
    def run(self):
        self.repo.experiments.clean()
        return 0


def add_parser(experiments_subparsers, parent_parser):
    EXPERIMENTS_CLEAN_HELP = "Cleanup experiments temporary internal files."
    experiments_clean_parser = experiments_subparsers.add_parser(
        "clean",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_CLEAN_HELP, "exp/clean"),
        help=EXPERIMENTS_CLEAN_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_clean_parser.set_defaults(func=CmdExperimentsClean)
