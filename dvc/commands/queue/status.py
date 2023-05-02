import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.compare import TabularData
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdQueueStatus(CmdBase):
    """Show queue task and worker status."""

    def run(self) -> int:
        from dvc.repo.experiments.show import format_time

        result = self.repo.experiments.celery_queue.status()
        if result:
            all_headers = ["Task", "Name", "Created", "Status"]
            td = TabularData(all_headers)
            for exp in result:
                created = format_time(exp.get("timestamp"))
                assert exp["rev"]
                assert exp["status"]
                td.append(
                    [
                        exp["rev"][:7],
                        exp.get("name") or "",
                        created,
                        exp["status"],
                    ]
                )
            td.render()
        else:
            ui.write("No experiment tasks in the queue.")
        ui.write()

        worker_status = self.repo.experiments.celery_queue.worker_status()
        active_count = len([name for name, task in worker_status.items() if task])
        idle_count = len(worker_status) - active_count

        ui.write(f"Worker status: {active_count} active, {idle_count} idle")

        return 0


def add_parser(queue_subparsers, parent_parser):
    QUEUE_STATUS_HELP = "Show the status of experiments queue tasks and workers."
    queue_status_parser = queue_subparsers.add_parser(
        "status",
        parents=[parent_parser],
        description=append_doc_link(QUEUE_STATUS_HELP, "queue/status"),
        help=QUEUE_STATUS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    queue_status_parser.set_defaults(func=CmdQueueStatus)
