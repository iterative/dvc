import pytest
from funcy import first

from dvc.exceptions import InvalidArgumentError


def to_dict(tasks):
    status_dict = {}
    for task in tasks:
        status_dict[task["name"]] = task["status"]
    return status_dict


@pytest.fixture
def queued_tasks(tmp_dir, dvc, scm, exp_stage):
    queue_length = 3
    name_list = []
    for i in range(queue_length):
        name = f"queued{i}"
        name_list.append(name)
        dvc.experiments.run(
            exp_stage.addressing,
            params=[f"foo={i+2*queue_length}"],
            queue=True,
            name=name,
        )
    return ["queued0", "queued1", "queued2"]


@pytest.fixture
def success_tasks(tmp_dir, dvc, scm, test_queue, exp_stage):
    queue_length = 3
    name_list = []
    for i in range(queue_length):
        name = f"success{i}"
        name_list.append(name)
        dvc.experiments.run(
            exp_stage.addressing, params=[f"foo={i}"], queue=True, name=name
        )
    dvc.experiments.run(run_all=True)
    return name_list


@pytest.fixture
def failed_tasks(tmp_dir, dvc, scm, test_queue, failed_exp_stage, capsys):
    queue_length = 3
    name_list = []
    for i in range(queue_length):
        name = f"failed{i}"
        name_list.append(name)
        dvc.experiments.run(
            failed_exp_stage.addressing,
            params=[f"foo={i+queue_length}"],
            queue=True,
            name=name,
        )
    dvc.experiments.run(run_all=True)
    output = capsys.readouterr()
    assert "Failed to reproduce experiment" in output.err
    return name_list


@pytest.mark.parametrize("follow", [True, False])
def test_celery_logs(
    tmp_dir,
    scm,
    dvc,
    failed_exp_stage,
    test_queue,
    follow,
    capsys,
):
    dvc.experiments.run(failed_exp_stage.addressing, queue=True)
    dvc.experiments.run(run_all=True)

    queue = dvc.experiments.celery_queue
    done_result = first(queue.iter_done())
    name = done_result.entry.stash_rev
    captured = capsys.readouterr()
    queue.logs(name, follow=follow)
    captured = capsys.readouterr()
    assert "failed to reproduce 'failed-copy-file'" in captured.out


def test_queue_status(dvc, failed_tasks, success_tasks, queued_tasks):
    assert len(dvc.experiments.stash_revs) == 3
    assert len(dvc.experiments.celery_queue.failed_stash) == 3
    status = to_dict(dvc.experiments.celery_queue.status())
    assert len(status) == 9
    for task in failed_tasks:
        assert status[task] == "Failed"
    for task in success_tasks:
        assert status[task] == "Success"
    for task in queued_tasks:
        assert status[task] == "Queued"


def test_queue_remove(dvc, failed_tasks, success_tasks, queued_tasks):
    assert len(dvc.experiments.stash_revs) == 3
    assert len(dvc.experiments.celery_queue.failed_stash) == 3
    assert len(dvc.experiments.celery_queue.status()) == 9

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.celery_queue.remove(failed_tasks[:2] + ["non-exist"])
    assert len(dvc.experiments.celery_queue.status()) == 9

    to_remove = failed_tasks[:2] + success_tasks[1:] + queued_tasks[1:2]
    assert set(dvc.experiments.celery_queue.remove(to_remove)) == set(
        to_remove
    )

    assert len(dvc.experiments.stash_revs) == 2
    assert len(dvc.experiments.celery_queue.failed_stash) == 1
    status = to_dict(dvc.experiments.celery_queue.status())
    assert set(status) == set(
        queued_tasks[:1]
        + queued_tasks[2:]
        + success_tasks[:1]
        + failed_tasks[2:]
    )
    assert status[queued_tasks[0]] == "Queued"
    assert status[queued_tasks[2]] == "Queued"

    assert (
        dvc.experiments.celery_queue.remove([], queued=True)
        == queued_tasks[:1] + queued_tasks[2:]
    )

    assert len(dvc.experiments.stash_revs) == 0
    assert len(dvc.experiments.celery_queue.failed_stash) == 1
    assert len(dvc.experiments.celery_queue.status()) == 2

    assert (
        dvc.experiments.celery_queue.remove([], failed=True)
        == failed_tasks[2:]
    )

    assert len(dvc.experiments.stash_revs) == 0
    assert len(dvc.experiments.celery_queue.failed_stash) == 0
    assert len(dvc.experiments.celery_queue.status()) == 1

    assert (
        dvc.experiments.celery_queue.remove([], success=True)
        == success_tasks[:1]
    )

    assert len(dvc.experiments.stash_revs) == 0
    assert len(dvc.experiments.celery_queue.failed_stash) == 0
    assert len(dvc.experiments.celery_queue.status()) == 0
