import time

import pytest
from celery import shared_task
from flaky.flaky_decorator import flaky

from dvc.exceptions import DvcException
from dvc.repo.experiments.exceptions import UnresolvedExpNamesError
from dvc.repo.experiments.queue.base import QueueDoneResult


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
        return_value=[(result, mock_entry)],
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


@pytest.mark.parametrize("status", ["FAILURE", "SUCCESS"])
def test_queue_iter_done_task(test_queue, mocker, status):

    mock_entry = mocker.Mock(stash_rev=_foo.name)

    result = mocker.Mock(status=status)

    mocker.patch.object(
        test_queue,
        "_iter_done_tasks",
        return_value=[(result, mock_entry)],
    )

    if status == "FAILURE":
        assert list(test_queue.iter_failed()) == [
            QueueDoneResult(mock_entry, None)
        ]

    elif status == "SUCCESS":
        with pytest.raises(DvcException):
            assert list(test_queue.iter_success())


def test_queue_status(test_queue, scm, mocker):
    from datetime import datetime

    active_entry = mocker.Mock(stash_rev="active")
    active_entry.name = "foo"
    queued_entry = mocker.Mock(stash_rev="queued")
    queued_entry.name = None
    failed_entry = mocker.Mock(stash_rev="failed")
    failed_entry.name = "bar"
    success_entry = mocker.Mock(stash_rev="success")
    success_entry.name = None
    success_result = mocker.Mock(ref_info=mocker.Mock())
    success_result.ref_info.name = "foobar"

    def resolve_commit(rev):
        if rev == "active":
            commit_time = datetime(2022, 8, 7).timestamp()
        elif rev == "queued":
            commit_time = datetime(2022, 8, 6).timestamp()
        elif rev == "failed":
            commit_time = datetime(2022, 8, 5).timestamp()
        elif rev == "success":
            commit_time = datetime(2022, 8, 4).timestamp()
        return mocker.Mock(commit_time=commit_time)

    mocker.patch.object(
        scm,
        "resolve_commit",
        side_effect=mocker.MagicMock(side_effect=resolve_commit),
    )

    mocker.patch.object(
        test_queue,
        "iter_active",
        return_value=[active_entry],
    )
    mocker.patch.object(
        test_queue,
        "iter_queued",
        return_value=[queued_entry],
    )
    mocker.patch.object(
        test_queue,
        "iter_failed",
        return_value=[(failed_entry, None)],
    )
    mocker.patch.object(
        test_queue,
        "iter_success",
        return_value=[(success_entry, success_result)],
    )

    assert test_queue.status() == [
        {
            "name": "foo",
            "rev": "active",
            "status": "Running",
            "timestamp": datetime(2022, 8, 7, 0, 0, 0),
        },
        {
            "name": None,
            "rev": "queued",
            "status": "Queued",
            "timestamp": datetime(2022, 8, 6, 0, 0, 0),
        },
        {
            "name": "bar",
            "rev": "failed",
            "status": "Failed",
            "timestamp": datetime(2022, 8, 5, 0, 0, 0),
        },
        {
            "name": "foobar",
            "rev": "success",
            "status": "Success",
            "timestamp": datetime(2022, 8, 4, 0, 0, 0),
        },
    ]
