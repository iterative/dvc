import pytest
from funcy import first

from dvc.exceptions import InvalidArgumentError


def to_dict(tasks):
    status_dict = {}
    for task in tasks:
        status_dict[task["name"]] = task["status"]
    return status_dict


@pytest.mark.parametrize("follow", [True, False])
def test_celery_logs(
    tmp_dir,
    scm,
    dvc,
    failed_exp_stage,
    follow,
    capsys,
):
    celery_queue = dvc.experiments.celery_queue
    dvc.experiments.run(failed_exp_stage.addressing, queue=True)
    dvc.experiments.run(run_all=True)

    done_result = first(celery_queue.iter_done())

    name = done_result.entry.stash_rev
    captured = capsys.readouterr()
    celery_queue.logs(name, follow=follow)
    captured = capsys.readouterr()
    assert "failed to reproduce 'failed-copy-file'" in captured.out


def test_queue_remove_done(
    dvc,
    exp_stage,
    failed_exp_stage,
):
    queue_length = 3
    success_tasks = []
    failed_tasks = []
    celery_queue = dvc.experiments.celery_queue
    for i in range(queue_length):
        name = f"success{i}"
        success_tasks.append(name)
        dvc.experiments.run(
            exp_stage.addressing, params=[f"foo={i}"], queue=True, name=name
        )
        name_fail = f"failed{i}"
        failed_tasks.append(name_fail)
        dvc.experiments.run(
            failed_exp_stage.addressing,
            params=[f"foo={i+queue_length}"],
            queue=True,
            name=name_fail,
        )
    dvc.experiments.run(run_all=True)
    assert len(celery_queue.failed_stash) == 3
    status = to_dict(celery_queue.status())
    assert len(status) == 6
    for name in failed_tasks:
        assert status[name] == "Failed"
    for name in success_tasks:
        assert status[name] == "Success"

    with pytest.raises(InvalidArgumentError):
        celery_queue.remove(failed_tasks[:2] + ["non-exist"])
    assert len(celery_queue.status()) == 6

    to_remove = [failed_tasks[0], success_tasks[2]]
    assert set(celery_queue.remove(to_remove)) == set(to_remove)

    assert len(celery_queue.failed_stash) == 2
    status = to_dict(celery_queue.status())
    assert set(status) == set(failed_tasks[1:] + success_tasks[:2])

    assert set(celery_queue.clear(failed=True)) == set(failed_tasks[1:])

    assert len(celery_queue.failed_stash) == 0
    assert set(to_dict(celery_queue.status())) == set(success_tasks[:2])

    assert set(celery_queue.clear(success=True)) == set(success_tasks[:2])

    assert celery_queue.status() == []
