from typing import TYPE_CHECKING

from dvc.ui import ui

if TYPE_CHECKING:
    from dvc.repo import Repo


def clean(repo: "Repo"):
    ui.write("Cleaning up dvc-task messages...")
    repo.experiments.celery_queue.celery.clean()
    ui.write("Done!")
