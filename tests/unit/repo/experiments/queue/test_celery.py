import time

import pytest
from celery import shared_task
from celery.result import AsyncResult

from dvc.exceptions import DvcException
from dvc.repo.experiments.exceptions import UnresolvedExpNamesError
from dvc.repo.experiments.queue.base import QueueDoneResult
from dvc.repo.experiments.queue.exceptions import CannotKillTasksError


def test_shutdown_no_tasks(test_queue, mocker):
    shutdown_spy = mocker.spy(test_queue.celery.control, "shutdown")
    test_queue.shutdown()
    shutdown_spy.assert_called_once()


@shared_task
def _foo(arg=None):
    return "foo"


def test_shutdown(test_queue, mocker):
    shutdown_spy = mocker.patch("celery.app.control.Control.shutdown")
    test_queue.shutdown()
    shutdown_spy.assert_called_once()


def test_shutdown_with_kill(test_queue, mocker):
    mock_entry_foo = mocker.Mock(stash_rev="af12de")
    mock_entry_foo.name = "foo"
    mock_entry_bar = mocker.Mock(stash_rev="bar")
    mock_entry_bar.name = None

    shutdown_spy = mocker.patch("celery.app.control.Control.shutdown")
    mocker.patch.object(
        test_queue,
        "iter_active",
        return_value=[mock_entry_foo, mock_entry_bar],
    )
    kill_spy = mocker.patch.object(test_queue, "_kill_entries")

    test_queue.shutdown(kill=True)

    shutdown_spy.assert_called_once()
    kill_spy.assert_called_once_with(
        {mock_entry_foo: "foo", mock_entry_bar: "bar"}, True
    )


def test_post_run_after_kill(test_queue):
    from celery import chain

    sig_bar = test_queue.proc.run_signature(
        ["python3", "-c", "import time; time.sleep(10)"], name="bar"
    )
    sig_bar.freeze()
    sig_foo = _foo.s()
    result_foo = sig_foo.freeze()
    run_chain = chain(sig_bar, sig_foo)

    run_chain.delay()
    timeout = time.time() + 10

    while True:
        try:
            test_queue.proc.kill("bar")
            assert result_foo.status == "PENDING"
            break
        except ProcessLookupError:
            time.sleep(0.1)
        if time.time() > timeout:
            raise TimeoutError

    assert result_foo.get(timeout=10) == "foo"


@pytest.mark.parametrize("force", [True, False])
def test_celery_queue_kill(test_queue, mocker, force):
    mock_entry_foo = mocker.Mock(stash_rev="foo")
    mock_entry_bar = mocker.Mock(stash_rev="bar")
    mock_entry_foobar = mocker.Mock(stash_rev="foobar")

    mocker.patch.object(
        test_queue,
        "iter_active",
        return_value={mock_entry_foo, mock_entry_bar, mock_entry_foobar},
    )
    mocker.patch.object(
        test_queue,
        "match_queue_entry_by_name",
        return_value={
            "bar": mock_entry_bar,
            "foo": mock_entry_foo,
            "foobar": mock_entry_foobar,
        },
    )
    mocker.patch.object(
        test_queue,
        "_get_running_task_ids",
        return_value={"foo", "foobar"},
    )
    mocker.patch.object(
        test_queue,
        "_iter_processed",
        return_value=[
            (mocker.Mock(headers={"id": "foo"}), mock_entry_foo),
            (mocker.Mock(headers={"id": "bar"}), mock_entry_bar),
            (mocker.Mock(headers={"id": "foobar"}), mock_entry_foobar),
        ],
    )
    mocker.patch.object(AsyncResult, "ready", return_value=False)
    mark_mocker = mocker.patch.object(test_queue.celery.backend, "mark_as_failure")

    def kill_function(rev):
        if rev == "foo":
            return True
        raise ProcessLookupError

    kill_mock = mocker.patch.object(
        test_queue.proc,
        "kill" if force else "interrupt",
        side_effect=mocker.MagicMock(side_effect=kill_function),
    )
    with pytest.raises(CannotKillTasksError, match="Task 'foobar' is initializing,"):
        test_queue.kill(["bar", "foo", "foobar"], force=force)
    assert kill_mock.call_args_list == [
        mocker.call(mock_entry_bar.stash_rev),
        mocker.call(mock_entry_foo.stash_rev),
        mocker.call(mock_entry_foobar.stash_rev),
    ]
    mark_mocker.assert_called_once_with("bar", None)


@pytest.mark.parametrize("force", [True, False])
def test_celery_queue_kill_invalid(test_queue, mocker, force):
    mock_entry_foo = mocker.Mock(stash_rev="foo")
    mock_entry_bar = mocker.Mock(stash_rev="bar")

    mocker.patch.object(
        test_queue,
        "match_queue_entry_by_name",
        return_value={"bar": mock_entry_bar, "foo": mock_entry_foo, "foobar": None},
    )

    kill_mock = mocker.patch.object(test_queue, "_kill_entries")

    with pytest.raises(UnresolvedExpNamesError):
        test_queue.kill(["bar", "foo", "foobar"], force=force)
    kill_mock.assert_called_once_with(
        {mock_entry_foo: "foo", mock_entry_bar: "bar"}, force
    )


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
        assert list(test_queue.iter_failed()) == [QueueDoneResult(mock_entry, None)]

    elif status == "SUCCESS":
        with pytest.raises(DvcException, match="Invalid experiment"):
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
            commit_time = datetime(2022, 8, 7).timestamp()  # noqa: DTZ001
        elif rev == "queued":
            commit_time = datetime(2022, 8, 6).timestamp()  # noqa: DTZ001
        elif rev == "failed":
            commit_time = datetime(2022, 8, 5).timestamp()  # noqa: DTZ001
        elif rev == "success":
            commit_time = datetime(2022, 8, 4).timestamp()  # noqa: DTZ001
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
            "timestamp": datetime(2022, 8, 7, 0, 0, 0),  # noqa: DTZ001
        },
        {
            "name": None,
            "rev": "queued",
            "status": "Queued",
            "timestamp": datetime(2022, 8, 6, 0, 0, 0),  # noqa: DTZ001
        },
        {
            "name": "bar",
            "rev": "failed",
            "status": "Failed",
            "timestamp": datetime(2022, 8, 5, 0, 0, 0),  # noqa: DTZ001
        },
        {
            "name": "foobar",
            "rev": "success",
            "status": "Success",
            "timestamp": datetime(2022, 8, 4, 0, 0, 0),  # noqa: DTZ001
        },
    ]
