import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link

logger = logging.getLogger(__name__)


class CmdQueueAttach(CmdBase):
    """Attach outputs of a exp task in queue."""

    def run(self):
        self.repo.experiments.celery_queue.attach(
            rev=self.args.experiment,
            encoding=self.args.encoding,
        )

        return 0


def add_parser(queue_subparsers, parent_parser):
    QUEUE_ATTACH_HELP = "Attach outputs of a experiment task in queue."
    queue_attach_parser = queue_subparsers.add_parser(
        "attach",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_ATTACH_HELP, "queue/attach"),
        help=QUEUE_ATTACH_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    queue_attach_parser.add_argument(
        "-e",
        "--encoding",
        help=(
            "Text encoding for redirected output. Defaults to"
            "`locale.getpreferredencoding()`."
        ),
    )
    queue_attach_parser.add_argument(
        "experiment",
        help="Experiments in queue to attach.",
        metavar="<experiment>",
    )
    queue_attach_parser.set_defaults(func=CmdQueueAttach)
