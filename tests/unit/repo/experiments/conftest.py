from functools import partial

import pytest

from dvc_task.app import FSApp

DEFAULT_ITERATIONS = 2


@pytest.fixture
def exp_stage(tmp_dir, scm, dvc, copy_script):
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="copy-file",
        deps=["copy.py"],
    )
    scm.add(
        [
            "dvc.yaml",
            "dvc.lock",
            "copy.py",
            "params.yaml",
            "metrics.yaml",
            ".gitignore",
        ]
    )
    scm.commit("init")
    return stage


@pytest.fixture
def failed_exp_stage(tmp_dir, scm, dvc, copy_script):
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.stage.add(
        cmd="python -c 'import sys; sys.exit(1)'",
        metrics_no_cache=["failed-metrics.yaml"],
        params=["foo"],
        name="failed-copy-file",
        deps=["copy.py"],
    )
    scm.add(
        [
            "dvc.yaml",
            "dvc.lock",
            "copy.py",
            "params.yaml",
            "failed-metrics.yaml",
            ".gitignore",
        ]
    )
    scm.commit("init")
    return stage


def _thread_worker(app, **kwargs):
    # Based on pytest-celery's celery_worker fixture but using thread pool
    # instead of solo pool so that broadcast/control API is available
    from celery.contrib.testing import worker

    app.loader.import_task_module("celery.contrib.testing.tasks")
    return worker.start_worker(app, pool="threads", **kwargs)


@pytest.fixture(scope="session")
def session_app(tmp_path_factory) -> FSApp:
    """Session scoped experiments queue celery app."""
    from kombu.transport.filesystem import Channel

    # related to https://github.com/iterative/dvc-task/issues/61
    Channel.QoS.restore_at_shutdown = False

    from dvc_task.app import FSApp

    wdir = tmp_path_factory.mktemp("dvc-test-celery")
    app = FSApp(
        "dvc-exp-local",
        wdir=wdir,
        mkdir=True,
        include=["dvc.repo.experiments.queue.tasks", "dvc_task.proc.tasks"],
    )
    app.conf.update({"task_acks_late": True, "result_expires": None})
    return app


@pytest.fixture(scope="session")
def session_worker(session_app):
    """Session scoped celery worker that runs in separate thread(s)."""
    with _thread_worker(
        session_app,
        concurrency=1,
        ping_task_timeout=20,
        loglevel="DEBUG",
    ) as worker:
        yield worker


@pytest.fixture
def session_queue(tmp_dir, dvc, scm, mocker, session_app, session_worker):
    """Patches experiments celery queue for pytest testing.

    Uses session-scoped celery worker.
    """
    queue = dvc.experiments.celery_queue
    queue.celery = session_app
    queue.worker = session_worker
    mocker.patch.object(queue, "_spawn_worker")
    return queue


@pytest.fixture
def test_queue(tmp_dir, dvc, scm, mocker):
    """Patches experiments celery queue for pytest testing.

    Uses function-scoped celery worker which runs in separate thread(s).
    """
    import celery

    queue = dvc.experiments.celery_queue
    mocker.patch.object(queue, "_spawn_worker")

    f = partial(_thread_worker, queue.celery, concurrency=1, ping_task_timeout=20)
    exc = None
    for _ in range(3):
        try:
            with f() as worker:
                mocker.patch.object(queue, "worker", return_value=worker)
                yield queue
                return
        except celery.exceptions.TimeoutError as e:
            exc = e
            continue
    assert exc
    raise exc
