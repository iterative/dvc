import logging

from dvc.cli.command import CmdBase

logger = logging.getLogger(__name__)


class CmdQueueWorker(CmdBase):
    """Run the exp queue worker."""

    def run(self):
        self.repo.experiments.celery_queue.worker.start(self.args.name)
        return 0


def add_parser(experiments_subparsers, parent_parser):
    QUEUE_WORKER_HELP = "Run the exp queue worker."
    parser = experiments_subparsers.add_parser(
        "queue-worker",
        parents=[parent_parser],
        description=QUEUE_WORKER_HELP,
        add_help=False,
    )
    parser.add_argument("name", help="Celery worker name.")
    parser.set_defaults(func=CmdQueueWorker)
