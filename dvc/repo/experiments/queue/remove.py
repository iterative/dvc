from typing import (
    TYPE_CHECKING,
    Collection,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Union,
)

from dvc.repo.experiments.exceptions import UnresolvedExpNamesError
from dvc.repo.experiments.queue.base import QueueDoneResult

if TYPE_CHECKING:
    from dvc.repo.experiments.queue.base import QueueEntry
    from dvc.repo.experiments.queue.local import LocalCeleryQueue
    from dvc.repo.experiments.stash import ExpStashEntry


def _remove_queued_tasks(
    celery_queue: "LocalCeleryQueue",
    queue_entries: Iterable[Optional["QueueEntry"]],
):
    """Remove tasks from task queue.

    Arguments:
        queue_entries: An iterable list of queued task to remove
    """
    stash_revs: Dict[str, "ExpStashEntry"] = {}
    for entry in queue_entries:
        if entry:
            stash_revs[entry.stash_rev] = celery_queue.stash.stash_revs[
                entry.stash_rev
            ]

    try:
        for (
            msg,
            queue_entry,
        ) in celery_queue._iter_queued():  # pylint: disable=protected-access
            if queue_entry.stash_rev in stash_revs:
                celery_queue.celery.reject(msg.delivery_tag)
    finally:
        celery_queue.stash.remove_revs(list(stash_revs.values()))


def _remove_done_tasks(
    celery_queue: "LocalCeleryQueue",
    queue_entries: Iterable[Optional["QueueEntry"]],
):
    """Remove done tasks.

    Arguments:
        queue_entries: An iterable list of done task to remove
    """
    from celery.result import AsyncResult

    failed_stash_revs: List["ExpStashEntry"] = []
    queue_entry_set: Set["QueueEntry"] = set()
    for entry in queue_entries:
        if entry:
            queue_entry_set.add(entry)
            if entry.stash_rev in celery_queue.failed_stash.stash_revs:
                failed_stash_revs.append(
                    celery_queue.failed_stash.stash_revs[entry.stash_rev]
                )

    try:
        for (
            msg,
            queue_entry,
        ) in (
            celery_queue._iter_processed()  # pylint: disable=protected-access
        ):
            if queue_entry not in queue_entry_set:
                continue
            task_id = msg.headers["id"]
            result: AsyncResult = AsyncResult(task_id)
            if result is not None:
                result.forget()
            celery_queue.celery.purge(msg.delivery_tag)
    finally:
        celery_queue.failed_stash.remove_revs(failed_stash_revs)


def _get_names(entries: Iterable[Union["QueueEntry", "QueueDoneResult"]]):
    names: List[str] = []
    for entry in entries:
        if isinstance(entry, QueueDoneResult):
            if entry.result and entry.result.ref_info:
                names.append(entry.result.ref_info.name)
                continue
            entry = entry.entry
        name = entry.name
        name = name or entry.stash_rev[:7]
        names.append(name)
    return names


def celery_clear(self: "LocalCeleryQueue", **kwargs) -> List[str]:
    """Remove entries from the queue.

    Arguments:
        queued: Remove all queued tasks.
        failed: Remove all failed tasks.
        success: Remove all success tasks.

    Returns:
        Revisions which were removed.
    """
    queued = kwargs.pop("queued", False)
    failed = kwargs.get("failed", False)
    success = kwargs.get("success", False)

    removed = []
    if queued:
        queue_entries = list(self.iter_queued())
        _remove_queued_tasks(self, queue_entries)
        removed.extend(_get_names(queue_entries))
    if failed or success:
        done_tasks: List["QueueDoneResult"] = []
        if failed:
            done_tasks.extend(self.iter_failed())
        if success:
            done_tasks.extend(self.iter_success())
        done_entries = [result.entry for result in done_tasks]
        _remove_done_tasks(self, done_entries)
        removed.extend(_get_names(done_tasks))

    return removed


def celery_remove(
    self: "LocalCeleryQueue",
    revs: Collection[str],
    queued: bool = False,
    failed: bool = False,
    success: bool = False,
    all_: bool = False,
) -> List[str]:
    """Remove the specified entries from the queue.

    Arguments:
        revs: Stash revisions or queued exp names to be removed.
        queued: Remove all queued tasks.
        failed: Remove all failed tasks.
        success: Remove all success tasks.
        all_: Remove all tasks.

    Returns:
        Revisions (or names) which were removed.
    """
    if all_:
        queued = failed = success = True
    if queued or failed or success:
        return self.clear(failed=failed, success=success, queued=queued)

    # match_queued
    queue_match_results = self.match_queue_entry_by_name(
        revs, self.iter_queued()
    )

    done_match_results = self.match_queue_entry_by_name(revs, self.iter_done())

    remained: List[str] = []
    removed: List[str] = []
    queued_to_remove: List["QueueEntry"] = []
    done_to_remove: List["QueueEntry"] = []
    for name in revs:
        done_match = done_match_results[name]
        if done_match:
            done_to_remove.append(done_match)
            removed.append(name)
            continue
        queue_match = queue_match_results[name]
        if queue_match:
            queued_to_remove.append(queue_match)
            removed.append(name)
            continue
        remained.append(name)

    if remained:
        raise UnresolvedExpNamesError(remained)

    if done_to_remove:
        _remove_done_tasks(self, done_to_remove)
    if queued_to_remove:
        _remove_queued_tasks(self, queued_to_remove)

    return removed
