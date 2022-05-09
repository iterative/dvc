from celery import shared_task


def test_shutdown_no_tasks(test_queue, mocker):
    shutdown_spy = mocker.spy(test_queue.celery.control, "shutdown")
    test_queue.shutdown()
    shutdown_spy.assert_called_once()


@shared_task
def _foo(arg="foo"):
    return arg


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


def test_post_run_after_kill(test_queue, mocker):
    import time

    from celery import chain

    sig_bar = test_queue.proc.run(
        ["python3", "-c", "import time; time.sleep(5)"], name="bar"
    )
    result_bar = sig_bar.freeze()
    sig_foo = _foo.s()
    result_foo = sig_foo.freeze()
    run_chain = chain(sig_bar, sig_foo)

    run_chain.delay()
    timeout = time.time() + 10

    while True:
        if result_bar.status == "RUNNING":
            break
        if time.time() > timeout:
            raise AssertionError()

    assert result_foo.status == "PENDING"
    test_queue.proc.kill("bar")

    while True:
        if result_foo.status == "SUCCESS":
            break
        if time.time() > timeout:
            raise AssertionError()
