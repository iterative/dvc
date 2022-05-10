import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdQueueRemove(CmdBase):
    """Remove exp in queue."""

    def run(self):
        if self.args.all:
            removed_list = self.repo.experiments.celery_queue.clear()
        else:
            removed_list = self.repo.experiments.celery_queue.remove(
                revs=self.args.experiment
            )

        removed = ", ".join(removed_list)
        ui.write(f"Removed experiments in queue: {removed}")

        return 0


def add_parser(queue_subparsers, parent_parser):

    QUEUE_REMOVE_HELP = "Remove experiments in queue"
    queue_remove_parser = queue_subparsers.add_parser(
        "remove",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_REMOVE_HELP, "queue/remove"),
        help=QUEUE_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    queue_remove_parser.add_argument(
        "--all", action="store_true", help="Remove all experiments in queue."
    )
    queue_remove_parser.add_argument(
        "experiment",
        nargs="*",
        help="Experiments in queue to remove.",
        metavar="<experiment>",
    )
    queue_remove_parser.set_defaults(func=CmdQueueRemove)
