from dvc.cli import formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


class CmdQueueStart(CmdBase):
    """Start exp queue workers."""

    def run(self):
        started = self.repo.experiments.celery_queue.start_workers(self.args.jobs)

        suffix = "s" if started > 1 else ""
        ui.write(f"Started '{started}' new experiments task queue worker{suffix}.")

        return 0


def add_parser(queue_subparsers, parent_parser):
    QUEUE_START_HELP = "Start the experiments task queue worker."
    queue_start_parser = queue_subparsers.add_parser(
        "start",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_START_HELP, "queue/start"),
        help=QUEUE_START_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    queue_start_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=1,
        help="Maximum number of concurrent queue workers to start. Defaults to 1.",
        metavar="<number>",
    )
    queue_start_parser.set_defaults(func=CmdQueueStart)
