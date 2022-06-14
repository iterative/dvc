from contextlib import contextmanager

import pytest

from dvc.repo.experiments.queue.celery import LocalCeleryQueue


@contextmanager
def _thread_worker(app, **kwargs):
    # Based on pytest-celery's celery_worker fixture but using thread pool
    # instead of solo pool so that broadcast/control API is available
    from celery.contrib.testing import worker

    app.loader.import_task_module("celery.contrib.testing.tasks")
    with worker.start_worker(app, pool="threads", **kwargs) as test_worker:
        yield test_worker


@pytest.fixture
def test_queue(tmp_dir, dvc, scm, mocker) -> LocalCeleryQueue:
    """Patches experiments celery queue for pytest testing.

    Test queue worker runs for the duration of the test in separate thread(s).
    """
    celery_queue = dvc.experiments.celery_queue
    mocker.patch.object(celery_queue, "spawn_worker")
    with _thread_worker(celery_queue.celery, concurrency=1) as worker:
        mocker.patch.object(celery_queue, "worker", return_value=worker)
        yield celery_queue
