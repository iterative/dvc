from contextlib import contextmanager
from textwrap import dedent

import pytest

from dvc.repo.experiments.queue.celery import LocalCeleryQueue
from tests.func.test_repro_multistage import COPY_SCRIPT

DEFAULT_ITERATIONS = 2
CHECKPOINT_SCRIPT_FORMAT = dedent(
    """\
    import os
    import sys
    import shutil

    from dvc.api import make_checkpoint

    checkpoint_file = {}
    checkpoint_iterations = int({})
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as fobj:
            try:
                value = int(fobj.read())
            except ValueError:
                value = 0
    else:
        with open(checkpoint_file, "w"):
            pass
        value = 0

    shutil.copyfile({}, {})

    if os.getenv("DVC_CHECKPOINT"):
        for _ in range(checkpoint_iterations):
            value += 1
            with open(checkpoint_file, "w") as fobj:
                fobj.write(str(value))
            make_checkpoint()
"""
)
CHECKPOINT_SCRIPT = CHECKPOINT_SCRIPT_FORMAT.format(
    "sys.argv[1]", "sys.argv[2]", "sys.argv[3]", "sys.argv[4]"
)


@pytest.fixture
def exp_stage(tmp_dir, scm, dvc):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
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
def checkpoint_stage(tmp_dir, scm, dvc, mocker):
    mocker.patch("dvc.stage.run.Monitor.AWAIT", 0.01)

    tmp_dir.gen("checkpoint.py", CHECKPOINT_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd=(
            f"python checkpoint.py foo {DEFAULT_ITERATIONS} "
            "params.yaml metrics.yaml"
        ),
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        checkpoints=["foo"],
        deps=["checkpoint.py"],
        no_exec=True,
        name="checkpoint-file",
    )
    scm.add(["dvc.yaml", "checkpoint.py", "params.yaml", ".gitignore"])
    scm.commit("init")
    stage.iterations = DEFAULT_ITERATIONS
    return stage


@pytest.fixture
def failed_exp_stage(tmp_dir, scm, dvc):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
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
    with _thread_worker(
        celery_queue.celery,
        concurrency=1,
        ping_task_timeout=30,
    ) as worker:
        mocker.patch.object(celery_queue, "worker", return_value=worker)
        yield celery_queue
