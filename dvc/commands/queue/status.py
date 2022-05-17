import argparse
import logging
from typing import List, Mapping, Optional

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.compare import TabularData

from ..experiments.show import format_time

logger = logging.getLogger(__name__)


class CmdQueueStatus(CmdBase):
    """Kill exp task in queue."""

    def run(self):
        result: List[
            Mapping[str, Optional[str]]
        ] = self.repo.experiments.celery_queue.status()
        all_headers = ["Rev", "Name", "Created", "Status"]
        td = TabularData(all_headers)
        for exp in result:
            created = format_time(exp.get("timestamp"))
            td.append(
                [exp["rev"], exp.get("name", ""), created, exp["status"]]
            )
        td.render()

        return 0


def add_parser(queue_subparsers, parent_parser):
    QUEUE_STATUS_HELP = "List the status of the queue tasks and workers"
    queue_status_parser = queue_subparsers.add_parser(
        "status",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_STATUS_HELP, "queue/status"),
        help=QUEUE_STATUS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    queue_status_parser.set_defaults(func=CmdQueueStatus)
