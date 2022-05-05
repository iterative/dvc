from celery import shared_task


def test_shutdown_no_tasks(test_queue, mocker):
    shutdown_spy = mocker.spy(test_queue.celery.control, "shutdown")
    test_queue.shutdown()
    shutdown_spy.assert_called_once()


@shared_task
def _foo():
    return "foo"


def test_shutdown_active_tasks(test_queue, mocker):

    shutdown_spy = mocker.spy(test_queue.celery.control, "shutdown")
    # use frozen signature to get an assigned task ID and AsyncResult object
    # before running the task
    sig = _foo.s()
    result = sig.freeze()
    mocker.patch.object(
        test_queue, "_iter_active_tasks", return_value=[(result.id, None)]
    )

    # shutdown signal handler should be registered but not executed since task
    # has not completed
    test_queue.shutdown()
    assert result.id in test_queue._shutdown_task_ids
    assert not result.ready()
    shutdown_spy.assert_not_called()

    sig.delay()
    # block until task completes
    assert "foo" == result.get()
    assert result.id not in test_queue._shutdown_task_ids
    shutdown_spy.assert_called_once()
