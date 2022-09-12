import pytest
from funcy import first

from dvc.exceptions import InvalidArgumentError


def to_dict(tasks):
    status_dict = {}
    for task in tasks:
        status_dict[task["name"]] = task["status"]
    return status_dict


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
def failed_tasks(tmp_dir, dvc, scm, test_queue, failed_exp_stage):
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
    return name_list


@pytest.mark.xfail(strict=False, reason="pytest-celery flaky")
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


@pytest.mark.xfail(strict=False, reason="pytest-celery flaky")
def test_queue_remove_done(dvc, failed_tasks, success_tasks):
    assert len(dvc.experiments.celery_queue.failed_stash) == 3
    status = to_dict(dvc.experiments.celery_queue.status())
    assert len(status) == 6
    for name in failed_tasks:
        assert status[name] == "Failed"
    for name in success_tasks:
        assert status[name] == "Success"

    with pytest.raises(InvalidArgumentError):
        dvc.experiments.celery_queue.remove(failed_tasks[:2] + ["non-exist"])
    assert len(dvc.experiments.celery_queue.status()) == 6

    to_remove = [failed_tasks[0], success_tasks[2]]
    assert set(dvc.experiments.celery_queue.remove(to_remove)) == set(
        to_remove
    )

    assert len(dvc.experiments.celery_queue.failed_stash) == 2
    status = to_dict(dvc.experiments.celery_queue.status())
    assert set(status) == set(failed_tasks[1:] + success_tasks[:2])

    assert dvc.experiments.celery_queue.clear(failed=True) == failed_tasks[1:]

    assert len(dvc.experiments.celery_queue.failed_stash) == 0
    assert set(to_dict(dvc.experiments.celery_queue.status())) == set(
        success_tasks[:2]
    )

    assert (
        dvc.experiments.celery_queue.clear(success=True) == success_tasks[:2]
    )

    assert dvc.experiments.celery_queue.status() == []
