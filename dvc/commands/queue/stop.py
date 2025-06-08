from dvc.cli import formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


class CmdQueueStop(CmdBase):
    """Stop exp queue workers."""

    def run(self):
        self.repo.experiments.celery_queue.shutdown(kill=self.args.kill)

        if self.args.kill:
            ui.write(
                "All running tasks in the queue have been killed."
                "Queue workers are stopping."
            )
        else:
            ui.write("Queue workers will stop after running tasks finish.")

        return 0


def add_parser(queue_subparsers, parent_parser):
    QUEUE_STOP_HELP = "Stop all experiments task queue workers."
    queue_stop_parser = queue_subparsers.add_parser(
        "stop",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_STOP_HELP, "queue/stop"),
        help=QUEUE_STOP_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    queue_stop_parser.add_argument(
        "--kill",
        action="store_true",
        help="Kill all running tasks before stopping the queue workers.",
    )
    queue_stop_parser.set_defaults(func=CmdQueueStop)
