import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link

logger = logging.getLogger(__name__)


class CmdQueueKill(CmdBase):
    """Kill exp task in queue."""

    def run(self):
        self.repo.experiments.celery_queue.kill(revs=self.args.task)

        return 0


def add_parser(queue_subparsers, parent_parser):
    QUEUE_KILL_HELP = "Kill actively running experiment queue tasks."
    queue_kill_parser = queue_subparsers.add_parser(
        "kill",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_KILL_HELP, "queue/kill"),
        help=QUEUE_KILL_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    queue_kill_parser.add_argument(
        "task",
        nargs="*",
        help="Tasks in queue to kill.",
        metavar="<task>",
    )
    queue_kill_parser.set_defaults(func=CmdQueueKill)
