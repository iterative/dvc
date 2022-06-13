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
                revs=self.args.task
            )

        if removed_list:
            removed = ", ".join(removed_list)
            ui.write(f"Removed tasks in queue: {removed}")
        else:
            ui.write(f"No tasks found in queue named {self.args.task}")

        return 0


def add_parser(queue_subparsers, parent_parser):

    QUEUE_REMOVE_HELP = "Remove tasks in experiments queue."
    queue_remove_parser = queue_subparsers.add_parser(
        "remove",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_REMOVE_HELP, "queue/remove"),
        help=QUEUE_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    queue_remove_parser.add_argument(
        "--all", action="store_true", help="Remove all tasks in queue."
    )
    queue_remove_parser.add_argument(
        "task",
        nargs="*",
        help="Tasks in queue to remove.",
        metavar="<task>",
    )
    queue_remove_parser.set_defaults(func=CmdQueueRemove)
