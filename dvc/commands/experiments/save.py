import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import DvcException
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdExperimentsSave(CmdBase):
    def run(self):
        try:
            ref = self.repo.experiments.save(
                name=self.args.name,
                force=self.args.force,
                include_untracked=self.args.include_untracked,
                message=self.args.message,
            )
        except DvcException:
            logger.exception("failed to save experiment")
            return 1

        if self.args.json:
            ui.write_json({"ref": ref})
        else:
            name = self.repo.experiments.get_exact_name([ref])[ref]
            ui.write(f"Experiment has been saved as: {name}")
            ui.write(
                "\nTo promote an experiment to a Git branch run:\n\n"
                "\tdvc exp branch <exp> <branch>\n"
            )

        return 0


def add_parser(experiments_subparsers, parent_parser):
    EXPERIMENTS_SAVE_HELP = "Save current workspace as an experiment."
    save_parser = experiments_subparsers.add_parser(
        "save",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_SAVE_HELP, "exp/save"),
        help=EXPERIMENTS_SAVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    save_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Replace experiment if it already exists.",
    )
    save_parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    save_parser.add_argument(
        "-n",
        "--name",
        default=None,
        help=(
            "Human-readable experiment name. If not specified, a name will "
            "be auto-generated."
        ),
        metavar="<name>",
    )
    save_parser.add_argument(
        "-I",
        "--include-untracked",
        action="append",
        default=[],
        help="List of untracked paths to include in the experiment.",
        metavar="<path>",
    )
    save_parser.add_argument(
        "-M",
        "--message",
        type=str,
        default=None,
        help="Custom commit message to use when committing the experiment.",
    )
    save_parser.set_defaults(func=CmdExperimentsSave)
