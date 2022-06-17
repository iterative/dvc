import argparse
import logging
from typing import List, Mapping, Optional

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.compare import TabularData
from dvc.ui import ui

from ..experiments.show import format_time

logger = logging.getLogger(__name__)


class CmdQueueStatus(CmdBase):
    """Kill exp task in queue."""

    def run(self):
        result: List[
            Mapping[str, Optional[str]]
        ] = self.repo.experiments.celery_queue.status()
        all_headers = ["Task", "Name", "Created", "Status"]
        td = TabularData(all_headers)
        for exp in result:
            created = format_time(exp.get("timestamp"))
            td.append(
                [exp["rev"][:7], exp.get("name", ""), created, exp["status"]]
            )
        td.render()

        if not result:
            ui.write("No experiments in task queue for now.")

        worker_status = self.repo.experiments.celery_queue.worker_status()
        active_count = len(
            [name for name, task in worker_status.items() if task]
        )
        idle_count = len(worker_status) - active_count

        if active_count == 1:
            ui.write("There is 1 worker active", end=", ")
        elif active_count == 0:
            ui.write("No worker active", end=", ")
        else:
            ui.write(f"There are {active_count} workers active", end=", ")

        if idle_count == 1:
            ui.write("1 worker idle at present.")
        elif idle_count == 0:
            ui.write("no worker idle at present.")
        else:
            ui.write(f"{idle_count} workers idle at present.")

        return 0


def add_parser(queue_subparsers, parent_parser):
    QUEUE_STATUS_HELP = "List the status of the queue tasks and workers."
    queue_status_parser = queue_subparsers.add_parser(
        "status",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_STATUS_HELP, "queue/status"),
        help=QUEUE_STATUS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    queue_status_parser.set_defaults(func=CmdQueueStatus)
