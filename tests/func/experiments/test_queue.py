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
    test_queue,
):
    celery_queue = dvc.experiments.celery_queue
    dvc.experiments.run(failed_exp_stage.addressing, queue=True, name="foo")
    dvc.experiments.run(run_all=True)
    test_queue.wait(["foo"])

    done_result = first(celery_queue.iter_done())

    name = done_result.entry.stash_rev
    captured = capsys.readouterr()
    celery_queue.logs(name, follow=follow)
    captured = capsys.readouterr()
    assert "failed to reproduce 'failed-copy-file'" in captured.out


@pytest.mark.xfail(
    strict=False,
    reason="https://github.com/iterative/dvc/issues/9143",
)
def test_queue_remove_done(
    dvc,
    exp_stage,
    failed_exp_stage,
    test_queue,
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
    test_queue.wait(success_tasks + failed_tasks)
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


def test_queue_doesnt_remove_untracked_params_file(tmp_dir, dvc, scm):
    """Regression test for https://github.com/iterative/dvc/issues/7842"""
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(cmd="echo ${foo}", params=["foo"], name="echo-foo")
    scm.add(
        [
            "dvc.yaml",
            "dvc.lock",
            ".gitignore",
        ]
    )
    scm.commit("init")
    dvc.experiments.run(stage.addressing, params=["foo=2"], queue=True)
    assert (tmp_dir / "params.yaml").exists()


def test_copy_paths_queue(tmp_dir, scm, dvc):
    stage = dvc.stage.add(
        cmd="cat file && ls dir",
        name="foo",
    )
    scm.add_commit(["dvc.yaml"], message="add dvc.yaml")

    (tmp_dir / "dir").mkdir()
    (tmp_dir / "dir" / "file").write_text("dir/file")
    scm.ignore(tmp_dir / "dir")
    (tmp_dir / "file").write_text("file")
    scm.ignore(tmp_dir / "file")

    dvc.experiments.run(stage.addressing, queue=True)
    results = dvc.experiments.run(run_all=True)

    exp = first(results)
    fs = scm.get_fs(exp)
    assert not fs.exists("dir")
    assert not fs.exists("file")
