import os
from typing import TYPE_CHECKING

from funcy import cached_property

from ..executor.base import EXEC_TMP_DIR
from .base import QueueEntry, StashQueue
from .tasks import setup_exp

if TYPE_CHECKING:
    from celery import Celery, Signature
    from dvc_task.worker import TemporaryWorker


class LocalCeleryQueue(StashQueue):
    """DVC experiment queue.

    Maps queued experiments to (Git) stash reflog entries.
    """

    CELERY_DIR = "celery"

    @cached_property
    def wdir(self) -> str:
        return os.path.join(self.repo.tmp_dir, EXEC_TMP_DIR, self.CELERY_DIR)

    @cached_property
    def celery(self) -> "Celery":
        from celery import Celery

        from .celery import get_config

        app = Celery()
        app.conf.update(get_config(self.wdir))
        return app

    @cached_property
    def worker(self) -> "TemporaryWorker":
        from dvc_task.worker import TemporaryWorker

        return TemporaryWorker(self.celery, concurrency=1)

    def put(self, *args, **kwargs) -> QueueEntry:
        """Stash an experiment and add it to the queue."""
        entry = super()._stash_exp(*args, **kwargs)
        # self.celery.tasks[exp_run.name].delay(entry.asdict())
        self.exp_chain(entry).delay()
        return entry

    def get(self) -> QueueEntry:
        """Pop and return the first item in the queue."""
        # Queue consumption should not be done directly. Celery worker(s) will
        # automatically consume available experiments.
        raise NotImplementedError

    def exp_chain(self, entry: QueueEntry) -> "Signature":
        from celery import chain

        chain(
            self.celery.tasks[setup_exp.name](entry.asdict()),
        )
