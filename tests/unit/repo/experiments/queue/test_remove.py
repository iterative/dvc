from dvc.repo.experiments.queue.base import QueueDoneResult


def test_remove_queued(test_queue, mocker):
    queued_test = ["queue1", "queue2", "queue3"]

    stash_dict = {}
    for name in queued_test:
        stash_dict[name] = mocker.Mock()

    msg_dict = {}
    entry_dict = {}
    for name in queued_test:
        msg_dict[name] = mocker.Mock(delivery_tag=f"msg_{name}")
        entry_dict[name] = mocker.Mock(stash_rev=name)
        entry_dict[name].name = name

    msg_iter = [(msg_dict[name], entry_dict[name]) for name in queued_test]
    entry_iter = [entry_dict[name] for name in queued_test]

    stash = mocker.patch.object(test_queue, "stash", return_value=mocker.Mock())
    stash.stash_revs = stash_dict
    mocker.patch.object(test_queue, "_iter_queued", return_value=msg_iter)
    mocker.patch.object(test_queue, "iter_queued", return_value=entry_iter)

    remove_revs_mocker = mocker.patch.object(test_queue.stash, "remove_revs")
    reject_mocker = mocker.patch.object(test_queue.celery, "reject")

    assert test_queue.remove(["queue2"]) == ["queue2"]
    reject_mocker.assert_called_once_with("msg_queue2")
    remove_revs_mocker.assert_called_once_with([stash_dict["queue2"]])
    remove_revs_mocker.reset_mock()
    reject_mocker.reset_mock()

    assert test_queue.clear(queued=True) == queued_test
    remove_revs_mocker.assert_called_once_with(list(stash_dict.values()))
    reject_mocker.assert_has_calls(
        [
            mocker.call("msg_queue1"),
            mocker.call("msg_queue2"),
            mocker.call("msg_queue3"),
        ]
    )


def test_remove_done(test_queue, mocker):
    from funcy import concat

    failed_test = ["failed1", "failed2", "failed3"]
    success_test = ["success1", "success2", "success3"]

    stash_dict = {}
    for name in failed_test:
        stash_dict[name] = mocker.Mock()

    msg_dict = {}
    entry_dict = {}
    for name in concat(failed_test, success_test):
        msg_dict[name] = mocker.Mock(delivery_tag=f"msg_{name}", headers={"id": 0})
        entry_dict[name] = mocker.Mock(stash_rev=name)
        entry_dict[name].name = name

    msg_iter = [
        (msg_dict[name], entry_dict[name]) for name in concat(failed_test, success_test)
    ]
    done_iter = [
        QueueDoneResult(entry_dict[name], None)
        for name in concat(failed_test, success_test)
    ]
    failed_iter = [QueueDoneResult(entry_dict[name], None) for name in failed_test]
    success_iter = [QueueDoneResult(entry_dict[name], None) for name in success_test]

    stash = mocker.patch.object(test_queue, "failed_stash", return_value=mocker.Mock())
    stash.stash_revs = stash_dict
    mocker.patch.object(test_queue, "_iter_processed", return_value=msg_iter)
    mocker.patch.object(test_queue, "iter_done", return_value=done_iter)
    mocker.patch.object(test_queue, "iter_success", return_value=success_iter)
    mocker.patch.object(test_queue, "iter_failed", return_value=failed_iter)
    mocker.patch("celery.result.AsyncResult", return_value=mocker.Mock())

    remove_revs_mocker = mocker.patch.object(test_queue.failed_stash, "remove_revs")
    purge_mocker = mocker.patch.object(test_queue.celery, "purge")

    assert test_queue.remove(["failed3", "success2"]) == ["failed3", "success2"]
    remove_revs_mocker.assert_called_once_with([stash_dict["failed3"]])
    purge_mocker.assert_has_calls(
        [mocker.call("msg_failed3"), mocker.call("msg_success2")]
    )

    remove_revs_mocker.reset_mock()
    purge_mocker.reset_mock()

    assert set(test_queue.clear(success=True, failed=True)) == set(failed_test) | set(
        success_test
    )
    purge_mocker.assert_has_calls(
        [
            mocker.call("msg_failed1"),
            mocker.call("msg_failed2"),
            mocker.call("msg_failed3"),
            mocker.call("msg_success1"),
            mocker.call("msg_success2"),
            mocker.call("msg_success3"),
        ],
        any_order=True,
    )
    remove_revs_mocker.assert_called_once_with(list(stash_dict.values()))
