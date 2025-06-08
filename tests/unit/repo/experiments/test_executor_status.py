import os

import pytest

from dvc.exceptions import ReproductionError
from dvc.repo.experiments.executor.base import ExecutorInfo, TaskStatus
from dvc.repo.experiments.queue.tasks import cleanup_exp, collect_exp, setup_exp


def test_celery_queue_success_status(dvc, scm, test_queue, exp_stage):
    queue_entry = test_queue._stash_exp(
        params={"params.yaml": ["foo=1"]},
        targets=exp_stage.addressing,
        name="success",
    )
    infofile = test_queue.get_infofile_path(queue_entry.stash_rev)
    executor = setup_exp.s(queue_entry.asdict())()
    executor_info = ExecutorInfo.load_json(infofile)
    assert executor_info.status == TaskStatus.PREPARING

    cmd = ["dvc", "exp", "exec-run", "--infofile", infofile]
    proc_dict = test_queue.proc.run_signature(cmd, name=queue_entry.stash_rev)()

    executor_info = ExecutorInfo.load_json(infofile)
    assert executor_info.status == TaskStatus.SUCCESS

    collect_exp.s(proc_dict, queue_entry.asdict())()
    cleanup_exp.s(executor, infofile)()
    executor_info = ExecutorInfo.load_json(infofile)
    assert executor_info.status == TaskStatus.FINISHED


def test_celery_queue_failure_status(dvc, scm, test_queue, failed_exp_stage):
    queue_entry = test_queue._stash_exp(
        params={"params.yaml": ["foo=1"]},
        targets=failed_exp_stage.addressing,
        name="failed",
    )
    infofile = test_queue.get_infofile_path(queue_entry.stash_rev)
    setup_exp.s(queue_entry.asdict())()
    cmd = ["dvc", "exp", "exec-run", "--infofile", infofile]
    test_queue.proc.run_signature(cmd, name=queue_entry.stash_rev)()
    executor_info = ExecutorInfo.load_json(infofile)
    assert executor_info.status == TaskStatus.FAILED


@pytest.mark.parametrize("queue_type", ["workspace_queue", "tempdir_queue"])
def test_workspace_executor_success_status(dvc, scm, exp_stage, queue_type):
    workspace_queue = getattr(dvc.experiments, queue_type)
    queue_entry = workspace_queue.put(
        params={"params.yaml": ["foo=1"]}, targets=exp_stage.addressing, name="success"
    )
    name = workspace_queue._EXEC_NAME or queue_entry.stash_rev
    infofile = workspace_queue.get_infofile_path(name)
    entry, executor = workspace_queue.get()
    rev = entry.stash_rev
    exec_result = executor.reproduce(info=executor.info, rev=rev, infofile=infofile)
    executor_info = ExecutorInfo.load_json(infofile)
    assert executor_info.status == TaskStatus.SUCCESS
    if exec_result.ref_info:
        workspace_queue.collect_executor(dvc.experiments, executor, exec_result)
    executor.cleanup(infofile)

    if queue_type == "tempdir_queue":
        executor_info = ExecutorInfo.load_json(infofile)
        assert executor_info.status == TaskStatus.FINISHED
    else:
        assert not os.path.exists(infofile)


@pytest.mark.parametrize("queue_type", ["workspace_queue", "tempdir_queue"])
def test_workspace_executor_failed_status(dvc, scm, failed_exp_stage, queue_type):
    queue = getattr(dvc.experiments, queue_type)
    queue.put(
        params={"params.yaml": ["foo=1"]},
        targets=failed_exp_stage.addressing,
        name="failed",
    )
    entry, executor = queue.get()
    name = queue._EXEC_NAME or entry.stash_rev
    infofile = queue.get_infofile_path(name)
    rev = entry.stash_rev

    with pytest.raises(ReproductionError):
        executor.reproduce(info=executor.info, rev=rev, infofile=infofile)
    executor_info = ExecutorInfo.load_json(infofile)
    assert executor_info.status == TaskStatus.FAILED

    cleanup_exp.s(executor, infofile)()
    if queue_type == "workspace_queue":
        assert not os.path.exists(infofile)
    else:
        executor_info = ExecutorInfo.load_json(infofile)
        assert executor_info.status == TaskStatus.FAILED


def test_executor_status_compatibility():
    data = {
        "git_url": "file:///Users/home",
        "baseline_rev": "123",
        "location": "dvc-task",
        "root_dir": "/Users/home/8088/.dvc/tmp/exps/tmpx85892cx",
        "dvc_dir": ".dvc",
        "collected": True,
    }
    result = ExecutorInfo.from_dict(data)
    assert result.status == TaskStatus.FINISHED
