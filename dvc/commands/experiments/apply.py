import argparse
import logging

from dvc.cli import completion
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdExperimentsApply(CmdBase):
    def run(self):
        if not self.args.force:
            ui.write(
                "The --no-force option is deprecated and will be removed in a future"
                " DVC release. To revert the result of 'exp apply', run:\n"
                "\n\tgit reset --hard\n"
                "\tgit stash apply refs/exps/apply/stash\n"
            )
        self.repo.experiments.apply(self.args.experiment)

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
        "--no-force",
        action="store_false",
        dest="force",
        help="Fail if this command would overwrite conflicting changes.",
    )
    experiments_apply_parser.add_argument(
        "experiment", help="Experiment to be applied."
    ).complete = completion.EXPERIMENT
    experiments_apply_parser.set_defaults(func=CmdExperimentsApply)
