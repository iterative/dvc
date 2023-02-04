from functools import partial
from textwrap import dedent

import pytest

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
        for index in range(checkpoint_iterations):
            value += 1
            {}
            with open(checkpoint_file, "w") as fobj:
                fobj.write(str(value))
            make_checkpoint()
"""
)
CHECKPOINT_SCRIPT = CHECKPOINT_SCRIPT_FORMAT.format(
    "sys.argv[1]", "sys.argv[2]", "sys.argv[3]", "sys.argv[4]", ""
)
FAILED_CHECKPOINT_SCRIPT = CHECKPOINT_SCRIPT_FORMAT.format(
    "sys.argv[1]",
    "sys.argv[2]",
    "sys.argv[3]",
    "sys.argv[4]",
    "if index == (checkpoint_iterations - 2): raise Exception",
)


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
def checkpoint_stage(tmp_dir, scm, dvc, mocker):
    mocker.patch("dvc.stage.run.Monitor.AWAIT", 0.01)

    tmp_dir.gen("checkpoint.py", CHECKPOINT_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd=f"python checkpoint.py foo {DEFAULT_ITERATIONS} params.yaml metrics.yaml",
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
def failed_checkpoint_stage(tmp_dir, scm, dvc, mocker):
    mocker.patch("dvc.stage.run.Monitor.AWAIT", 0.01)

    tmp_dir.gen("checkpoint.py", FAILED_CHECKPOINT_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd=f"python checkpoint.py foo {DEFAULT_ITERATIONS+2} params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        checkpoints=["foo"],
        deps=["checkpoint.py"],
        no_exec=True,
        name="failed-checkpoint-file",
    )
    scm.add(["dvc.yaml", "checkpoint.py", "params.yaml", ".gitignore"])
    scm.commit("init")
    stage.iterations = DEFAULT_ITERATIONS
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


@pytest.fixture
def test_queue(tmp_dir, dvc, scm, mocker):
    """Patches experiments celery queue for pytest testing.

    Test queue worker runs for the duration of the test in separate thread(s).
    """
    import celery

    queue = dvc.experiments.celery_queue
    mocker.patch.object(queue, "_spawn_worker")

    f = partial(
        _thread_worker,
        queue.celery,
        concurrency=1,
        ping_task_timeout=20,
    )
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
