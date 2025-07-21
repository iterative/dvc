import pytest
from funcy import first


def to_dict(tasks):
    status_dict = {}
    for task in tasks:
        status_dict[task["name"]] = task["status"]
    return status_dict


@pytest.mark.parametrize("follow", [True, False])
def test_celery_logs(tmp_dir, scm, dvc, failed_exp_stage, follow, capsys, test_queue):
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


def test_queue_doesnt_remove_untracked_params_file(tmp_dir, dvc, scm):
    """Regression test for https://github.com/iterative/dvc/issues/7842"""
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(cmd="echo ${foo}", params=["foo"], name="echo-foo")
    scm.add(["dvc.yaml", "dvc.lock", ".gitignore"])
    scm.commit("init")
    dvc.experiments.run(stage.addressing, params=["foo=2"], queue=True)
    assert (tmp_dir / "params.yaml").exists()


def test_copy_paths_queue(tmp_dir, scm, dvc):
    stage = dvc.stage.add(cmd="cat file && ls dir", name="foo")
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


def test_custom_commit_message_queue(tmp_dir, scm, dvc):
    stage = dvc.stage.add(cmd="echo foo", name="foo")
    scm.add_commit(["dvc.yaml"], message="add dvc.yaml")

    dvc.experiments.run(stage.addressing, queue=True, message="custom commit message")

    exp = first(dvc.experiments.run(run_all=True))
    assert scm.resolve_commit(exp).message == "custom commit message"
