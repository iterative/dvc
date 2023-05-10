import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import InvalidArgumentError
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdQueueRemove(CmdBase):
    """Remove exp in queue."""

    def check_arguments(self):
        clear_flag = any(
            [
                self.args.all,
                self.args.queued,
                self.args.failed,
                self.args.success,
            ]
        )
        if not (clear_flag ^ bool(self.args.task)):
            raise InvalidArgumentError(
                "Either provide an `tasks` argument, or use the "
                "`--all`, `--queued`, `--failed`, `--success` flag."
            )

    def run(self):
        self.check_arguments()

        if self.args.all:
            self.args.queued = True
            self.args.failed = True
            self.args.success = True

        if self.args.queued or self.args.failed or self.args.success:
            removed_list = self.repo.experiments.celery_queue.clear(
                success=self.args.success,
                queued=self.args.queued,
                failed=self.args.failed,
            )
        else:
            removed_list = self.repo.experiments.celery_queue.remove(
                revs=self.args.task,
            )

        if removed_list:
            removed = ", ".join(removed_list)
            ui.write(f"Removed tasks in queue: {removed}")
        else:
            ui.write(f"No tasks found named {self.args.task}")

        return 0


def add_parser(queue_subparsers, parent_parser):
    QUEUE_REMOVE_HELP = "Remove queued and completed tasks from the queue."
    queue_remove_parser = queue_subparsers.add_parser(
        "remove",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_REMOVE_HELP, "queue/remove"),
        help=QUEUE_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    queue_remove_parser.add_argument(
        "--all",
        action="store_true",
        help="Remove all queued and completed tasks from the queue.",
    )
    queue_remove_parser.add_argument(
        "--queued",
        action="store_true",
        help="Remove all queued tasks from the queue.",
    )
    queue_remove_parser.add_argument(
        "--success",
        action="store_true",
        help="Remove all successful tasks from the queue.",
    )
    queue_remove_parser.add_argument(
        "--failed",
        action="store_true",
        help="Remove all failed tasks from the queue.",
    )
    queue_remove_parser.add_argument(
        "task",
        nargs="*",
        help="Tasks to remove.",
        metavar="<task>",
    )
    queue_remove_parser.set_defaults(func=CmdQueueRemove)
