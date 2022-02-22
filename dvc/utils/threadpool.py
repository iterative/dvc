from concurrent import futures
from itertools import islice
from typing import Any, Callable, Iterable, Iterator, Set, TypeVar

_T = TypeVar("_T")


class ThreadPoolExecutor(futures.ThreadPoolExecutor):
    _max_workers: int

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def imap_unordered(
        self,
        fn: Callable[..., _T],
        *iterables: Iterable[Any],
        cancel_futures: bool = False,
    ) -> Iterator[_T]:
        """Lazier version of map that does not preserve ordering of results.

        It does not create all the futures at once to reduce memory usage.

        If cancel_futures is True and a future in the taskset raises an
        exception, any unfinished futures in the taskset will be cancelled
        before the exception is re-raised to the executor.
        """

        def create_taskset(n: int) -> Set[futures.Future]:
            return {self.submit(fn, *args) for args in islice(it, n)}

        it = zip(*iterables)
        tasks = create_taskset(self.max_workers * 5)
        while tasks:
            done, tasks = futures.wait(
                tasks, return_when=futures.FIRST_COMPLETED
            )
            try:
                for fut in done:
                    yield fut.result()
            except Exception:
                if cancel_futures:
                    for fut in tasks:
                        fut.cancel()
                raise
            tasks.update(create_taskset(len(done)))
