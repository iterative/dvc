from typing import TYPE_CHECKING, Collection, Dict, Iterable, List, Set, Union

from dvc.repo.experiments.exceptions import UnresolvedExpNamesError
from dvc.repo.experiments.queue.base import QueueDoneResult

if TYPE_CHECKING:
    from dvc.repo.experiments.queue.base import QueueEntry
    from dvc.repo.experiments.queue.celery import LocalCeleryQueue
    from dvc.repo.experiments.stash import ExpStashEntry


def remove_tasks(  # noqa: C901, PLR0912
    celery_queue: "LocalCeleryQueue",
    queue_entries: Iterable["QueueEntry"],
):
    """Remove tasks from task queue.

    Arguments:
        queue_entries: An iterable list of task to remove
    """
    from celery.result import AsyncResult

    stash_revs: Dict[str, "ExpStashEntry"] = {}
    failed_stash_revs: List["ExpStashEntry"] = []
    done_entry_set: Set["QueueEntry"] = set()
    stash_rev_all = celery_queue.stash.stash_revs
    failed_rev_all: Dict[str, "ExpStashEntry"] = {}
    if celery_queue.failed_stash:
        failed_rev_all = celery_queue.failed_stash.stash_revs
    for entry in queue_entries:
        if entry.stash_rev in stash_rev_all:
            stash_revs[entry.stash_rev] = stash_rev_all[entry.stash_rev]
        else:
            done_entry_set.add(entry)
            if entry.stash_rev in failed_rev_all:
                failed_stash_revs.append(failed_rev_all[entry.stash_rev])

    try:
        for (
            msg,
            queue_entry,
        ) in celery_queue._iter_queued():
            if queue_entry.stash_rev in stash_revs and msg.delivery_tag:
                celery_queue.celery.reject(msg.delivery_tag)
    finally:
        celery_queue.stash.remove_revs(list(stash_revs.values()))

    try:
        for (
            msg,
            queue_entry,
        ) in celery_queue._iter_processed():
            if queue_entry not in done_entry_set:
                continue
            task_id = msg.headers["id"]
            result: AsyncResult = AsyncResult(task_id)
            if result is not None:
                result.forget()
            if msg.delivery_tag:
                celery_queue.celery.purge(msg.delivery_tag)
    finally:
        if celery_queue.failed_stash:
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


def celery_clear(
    self: "LocalCeleryQueue",
    queued: bool = False,
    failed: bool = False,
    success: bool = False,
) -> List[str]:
    """Remove entries from the queue.

    Arguments:
        queued: Remove all queued tasks.
        failed: Remove all failed tasks.
        success: Remove all success tasks.

    Returns:
        Revisions which were removed.
    """

    removed: List[str] = []
    entry_list: List["QueueEntry"] = []
    if queued:
        queue_entries: List["QueueEntry"] = list(self.iter_queued())
        entry_list.extend(queue_entries)
        removed.extend(_get_names(queue_entries))
    if failed:
        failed_tasks: List["QueueDoneResult"] = list(self.iter_failed())
        entry_list.extend([result.entry for result in failed_tasks])
        removed.extend(_get_names(failed_tasks))
    if success:
        success_tasks: List["QueueDoneResult"] = list(self.iter_success())
        entry_list.extend([result.entry for result in success_tasks])
        removed.extend(_get_names(success_tasks))

    remove_tasks(self, entry_list)

    return removed


def celery_remove(
    self: "LocalCeleryQueue",
    revs: Collection[str],
) -> List[str]:
    """Remove the specified entries from the queue.

    Arguments:
        revs: Stash revisions or queued exp names to be removed.

    Returns:
        Revisions (or names) which were removed.
    """

    match_results = self.match_queue_entry_by_name(
        revs, self.iter_queued(), self.iter_done()
    )

    remained: List[str] = []
    removed: List[str] = []
    entry_to_remove: List["QueueEntry"] = []
    for name, entry in match_results.items():
        if entry:
            entry_to_remove.append(entry)
            removed.append(name)
        else:
            remained.append(name)

    if remained:
        raise UnresolvedExpNamesError(remained)

    if entry_to_remove:
        remove_tasks(self, entry_to_remove)

    return removed
