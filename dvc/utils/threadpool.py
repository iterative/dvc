import queue
import sys
from concurrent import futures
from itertools import islice
from typing import Any, Callable, Iterable, Iterator, Optional, Set, TypeVar

_T = TypeVar("_T")


class ThreadPoolExecutor(futures.ThreadPoolExecutor):
    _max_workers: int

    def __init__(
        self,
        max_workers: Optional[int] = None,
        cancel_on_error: bool = False,
        **kwargs,
    ):
        super().__init__(max_workers=max_workers, **kwargs)
        self._cancel_on_error = cancel_on_error

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def imap_unordered(
        self, fn: Callable[..., _T], *iterables: Iterable[Any]
    ) -> Iterator[_T]:
        """Lazier version of map that does not preserve ordering of results.

        It does not create all the futures at once to reduce memory usage.
        """

        def create_taskset(n: int) -> Set[futures.Future]:
            return {self.submit(fn, *args) for args in islice(it, n)}

        it = zip(*iterables)
        tasks = create_taskset(self.max_workers * 5)
        while tasks:
            done, tasks = futures.wait(tasks, return_when=futures.FIRST_COMPLETED)
            for fut in done:
                yield fut.result()
            tasks.update(create_taskset(len(done)))

    def shutdown(self, wait=True, *, cancel_futures=False):
        if sys.version_info > (3, 9):  # pylint: disable=no-else-return
            # pylint: disable=unexpected-keyword-arg
            return super().shutdown(wait=wait, cancel_futures=cancel_futures)
        else:  # noqa: RET505
            with self._shutdown_lock:
                self._shutdown = True
                if cancel_futures:
                    # Drain all work items from the queue, and then cancel
                    # their associated futures.
                    while True:
                        try:
                            work_item = self._work_queue.get_nowait()
                        except queue.Empty:
                            break
                        if work_item is not None:
                            work_item.future.cancel()

                # Send a wake-up to prevent threads calling
                # _work_queue.get(block=True) from permanently blocking.
                self._work_queue.put(None)  # type: ignore[arg-type]
            if wait:
                for t in self._threads:
                    t.join()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._cancel_on_error:
            self.shutdown(wait=True, cancel_futures=exc_val is not None)
        else:
            self.shutdown(wait=True)
        return False
