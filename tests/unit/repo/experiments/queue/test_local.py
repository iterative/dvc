import time

import pytest
from celery import shared_task
from flaky.flaky_decorator import flaky

from dvc.repo.experiments.exceptions import UnresolvedExpNamesError
from dvc.repo.experiments.executor.local import TempDirExecutor
from dvc.repo.experiments.refs import EXEC_BASELINE, EXEC_HEAD, EXEC_MERGE


def test_shutdown_no_tasks(test_queue, mocker):
    shutdown_spy = mocker.spy(test_queue.celery.control, "shutdown")
    test_queue.shutdown()
    shutdown_spy.assert_called_once()


@shared_task
def _foo(arg=None):  # pylint: disable=unused-argument
    return "foo"


def test_shutdown(test_queue, mocker):
    shutdown_spy = mocker.patch("celery.app.control.Control.shutdown")
    test_queue.shutdown()
    shutdown_spy.assert_called_once()


def test_shutdown_with_kill(test_queue, mocker):

    sig = _foo.s()
    mock_entry = mocker.Mock(stash_rev=_foo.name)

    result = sig.freeze()

    shutdown_spy = mocker.patch("celery.app.control.Control.shutdown")
    mocker.patch.object(
        test_queue,
        "_iter_active_tasks",
        return_value=[(result.id, mock_entry)],
    )
    kill_spy = mocker.patch.object(test_queue.proc, "kill")

    test_queue.shutdown(kill=True)

    sig.delay()

    assert result.get() == "foo"
    assert result.id not in test_queue._shutdown_task_ids
    kill_spy.assert_called_once_with(mock_entry.stash_rev)
    shutdown_spy.assert_called_once()


# pytest-celery worker thread may finish the task before we check for PENDING
@flaky(max_runs=3, min_passes=1)
def test_post_run_after_kill(test_queue):

    from celery import chain

    sig_bar = test_queue.proc.run_signature(
        ["python3", "-c", "import time; time.sleep(5)"], name="bar"
    )
    result_bar = sig_bar.freeze()
    sig_foo = _foo.s()
    result_foo = sig_foo.freeze()
    run_chain = chain(sig_bar, sig_foo)

    run_chain.delay()
    timeout = time.time() + 10

    while True:
        if result_bar.status == "STARTED" or result_bar.ready():
            break
        if time.time() > timeout:
            raise AssertionError()

    assert result_foo.status == "PENDING"
    test_queue.proc.kill("bar")

    assert result_foo.get(timeout=10) == "foo"


def test_celery_queue_kill(test_queue, mocker):

    mock_entry = mocker.Mock(stash_rev=_foo.name)

    mocker.patch.object(
        test_queue,
        "iter_active",
        return_value={mock_entry},
    )
    mocker.patch.object(
        test_queue,
        "match_queue_entry_by_name",
        return_value={"bar": None},
    )
    with pytest.raises(UnresolvedExpNamesError):
        test_queue.kill("bar")

    mocker.patch.object(
        test_queue,
        "match_queue_entry_by_name",
        return_value={"bar": mock_entry},
    )

    spy = mocker.patch.object(test_queue.proc, "kill")
    test_queue.kill("bar")
    assert spy.called_once_with(mock_entry.stash_rev)


def test_queue_clean_workspace_refs(git_dir, tmp_dir):

    rev = git_dir.scm.get_rev()
    exec_heads = (EXEC_MERGE, EXEC_BASELINE, EXEC_HEAD)
    for ref in exec_heads:
        git_dir.scm.set_ref(ref, rev)

    temp_dir_executor = TempDirExecutor(str(tmp_dir), str(tmp_dir), rev)
    temp_dir_executor.init_git(git_dir.scm)

    for ref in exec_heads:
        assert git_dir.scm.get_ref(ref) is None
