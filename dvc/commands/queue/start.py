import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdQueueStart(CmdBase):
    """Start exp queue workers."""

    def run(self):
        for _ in range(self.args.jobs):
            self.repo.experiments.celery_queue.spawn_worker()

        suffix = "s" if self.args.jobs > 1 else ""
        ui.write(
            f"Start {self.args.jobs} worker{suffix} to process the queue."
        )

        return 0


def job_type(job):
    try:
        job = int(job)
        if job > 0:
            return job
    except ValueError:
        pass
    raise argparse.ArgumentTypeError("Worker number must be a natural number.")


def add_parser(queue_subparsers, parent_parser):

    QUEUE_START_HELP = "Start experiments queue workers."
    queue_start_parser = queue_subparsers.add_parser(
        "start",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_START_HELP, "queue/start"),
        help=QUEUE_START_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    queue_start_parser.add_argument(
        "-j",
        "--jobs",
        type=job_type,
        default=1,
        help="Number of queue workers to start.",
    )
    queue_start_parser.set_defaults(func=CmdQueueStart)
