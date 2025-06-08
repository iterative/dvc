from collections.abc import Iterable, Iterator
from concurrent import futures
from itertools import islice
from typing import Any, Callable, Optional, TypeVar

_T = TypeVar("_T")


class ThreadPoolExecutor(futures.ThreadPoolExecutor):
    def __init__(
        self,
        max_workers: Optional[int] = None,
        cancel_on_error: bool = False,
        **kwargs,
    ):
        super().__init__(max_workers=max_workers, **kwargs)
        self._cancel_on_error = cancel_on_error

    def imap_unordered(
        self, fn: Callable[..., _T], *iterables: Iterable[Any]
    ) -> Iterator[_T]:
        """Lazier version of map that does not preserve ordering of results.

        It does not create all the futures at once to reduce memory usage.
        """

        def create_taskset(n: int) -> set[futures.Future]:
            return {self.submit(fn, *args) for args in islice(it, n)}

        it = zip(*iterables)
        tasks = create_taskset(self._max_workers * 5)
        while tasks:
            done, tasks = futures.wait(tasks, return_when=futures.FIRST_COMPLETED)
            for fut in done:
                yield fut.result()
            tasks.update(create_taskset(len(done)))

    def __exit__(self, exc_type, exc_val, exc_tb):
        cancel_futures = self._cancel_on_error and exc_val is not None
        self.shutdown(wait=True, cancel_futures=cancel_futures)
        return False
