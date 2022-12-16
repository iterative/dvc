import operator
import time

import pytest
from funcy import raiser

from dvc.utils.threadpool import ThreadPoolExecutor


@pytest.mark.parametrize("wait", [True, False])
@pytest.mark.parametrize("cancel_futures", [True, False])
def test_cancel_futures(wait, cancel_futures):
    """Modified from
    https://github.com/python/cpython/blob/4d2403f/Lib/test/test_concurrent_futures.py#L354
    """
    executor = ThreadPoolExecutor(max_workers=2)
    fs = [executor.submit(time.sleep, 0.1) for _ in range(50)]
    executor.shutdown(wait=wait, cancel_futures=cancel_futures)

    if not wait:
        for t in executor._threads:
            t.join()

    cancelled = [fut for fut in fs if fut.cancelled()]
    # Use "not fut.cancelled()" instead of "fut.done()" to include futures
    # that may have been left in a pending state.
    others = [fut for fut in fs if not fut.cancelled()]

    # Ensure the other futures were able to finish.
    for fut in others:
        assert fut.done()
        assert fut.exception() is None

    if not cancel_futures:
        # there should be no cancelled futures
        assert len(cancelled) == 0
        assert len(others) == len(fs)
    else:
        # We can't guarantee the exact number of cancellations, but we can
        # guarantee that *some* were cancelled. With few workers, many of
        # the submitted futures should have been cancelled.
        assert len(cancelled) > 20
        # Similar to the number of cancelled futures, we can't guarantee the
        # exact number that completed. But, we can guarantee that at least
        # one finished.
        assert len(others) > 0


def test_cancel_on_error_context_manager(mocker):
    executor = ThreadPoolExecutor(max_workers=2, cancel_on_error=True)
    spy = mocker.spy(executor, "shutdown")
    with pytest.raises(RuntimeError), executor:
        future1 = executor.submit(operator.mul, 2, 21)
        future2 = executor.submit(time.sleep, 0.1)
        future3 = executor.submit(raiser(RuntimeError), "This is an error")
        fs = [executor.submit(time.sleep, 0.1) for _ in range(50)]

        assert future1.result() == 42
        assert future2.result() is None
        _ = future3.result()

    spy.assert_called_once_with(wait=True, cancel_futures=True)

    cancelled = [fut for fut in fs if fut.cancelled()]
    others = [fut for fut in fs if not fut.cancelled()]

    for fut in others:
        assert fut.done()
        assert fut.exception() is None

    assert len(cancelled) > 20
    assert len(others) > 0
