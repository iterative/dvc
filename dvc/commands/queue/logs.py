import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link

logger = logging.getLogger(__name__)


class CmdQueueLogs(CmdBase):
    """Show output logs for a queued experiment."""

    def run(self):
        self.repo.experiments.celery_queue.logs(
            rev=self.args.experiment,
            encoding=self.args.encoding,
            follow=self.args.follow,
        )

        return 0


def add_parser(queue_subparsers, parent_parser):
    QUEUE_LOGS_HELP = "Show output logs for a queued experiment."
    queue_logs_parser = queue_subparsers.add_parser(
        "logs",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_LOGS_HELP, "queue/logs"),
        help=QUEUE_LOGS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    queue_logs_parser.add_argument(
        "-e",
        "--encoding",
        help=(
            "Text encoding for log output. Defaults to system locale encoding."
        ),
    )
    queue_logs_parser.add_argument(
        "-f",
        "--follow",
        help=(
            "Attach to experiment and follow additional live output. Only "
            "applicable if the experiment is still running."
        ),
        action="store_true",
    )
    queue_logs_parser.add_argument(
        "experiment",
        help="Experiment to show.",
        metavar="<experiment>",
    )
    queue_logs_parser.set_defaults(func=CmdQueueLogs)
