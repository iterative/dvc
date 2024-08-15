from collections.abc import Iterable
from typing import TYPE_CHECKING, Optional, Union

from dvc.log import logger
from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import Git, iter_revs

from .exceptions import UnresolvedExpNamesError
from .utils import exp_refs, exp_refs_by_baseline, push_refspec

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.repo.experiments.queue.celery import LocalCeleryQueue

    from .queue.base import ExpRefAndQueueEntry, QueueEntry
    from .refs import ExpRefInfo


logger = logger.getChild(__name__)


@locked
@scm_context
def remove(  # noqa: C901, PLR0912
    repo: "Repo",
    exp_names: Union[None, str, list[str]] = None,
    rev: Optional[Union[list[str], str]] = None,
    all_commits: bool = False,
    num: int = 1,
    queue: bool = False,
    git_remote: Optional[str] = None,
) -> list[str]:
    removed: list[str] = []
    if not any([exp_names, queue, all_commits, rev]):
        return removed
    celery_queue: LocalCeleryQueue = repo.experiments.celery_queue

    if queue:
        removed.extend(celery_queue.clear(queued=True))

    assert isinstance(repo.scm, Git)

    exp_ref_list: list[ExpRefInfo] = []
    queue_entry_list: list[QueueEntry] = []
    if exp_names:
        results: dict[str, ExpRefAndQueueEntry] = (
            celery_queue.get_ref_and_entry_by_names(exp_names, git_remote)
        )
        remained: list[str] = []
        for name, result in results.items():
            if not result.exp_ref_info and not result.queue_entry:
                remained.append(name)
                continue
            removed.append(name)
            if result.exp_ref_info:
                exp_ref_list.append(result.exp_ref_info)
            if result.queue_entry:
                queue_entry_list.append(result.queue_entry)

        if remained:
            raise UnresolvedExpNamesError(remained, git_remote=git_remote)
    elif rev:
        if isinstance(rev, str):
            rev = [rev]
        exp_ref_dict = _resolve_exp_by_baseline(repo, rev, num, git_remote)
        removed.extend(exp_ref_dict.keys())
        exp_ref_list.extend(exp_ref_dict.values())
    elif all_commits:
        exp_ref_list.extend(exp_refs(repo.scm, git_remote))
        removed = [ref.name for ref in exp_ref_list]

    if exp_ref_list:
        _remove_commited_exps(repo.scm, exp_ref_list, git_remote)

    if queue_entry_list:
        from .queue.remove import remove_tasks

        remove_tasks(celery_queue, queue_entry_list)

    if git_remote:
        from .push import notify_refs_to_studio

        removed_refs = [str(r) for r in exp_ref_list]
        notify_refs_to_studio(repo, git_remote, removed=removed_refs)
    return removed


def _resolve_exp_by_baseline(
    repo: "Repo",
    rev: list[str],
    num: int,
    git_remote: Optional[str] = None,
) -> dict[str, "ExpRefInfo"]:
    assert isinstance(repo.scm, Git)

    commit_ref_dict: dict[str, ExpRefInfo] = {}
    rev_dict = iter_revs(repo.scm, rev, num)
    rev_set = set(rev_dict.keys())
    ref_info_dict = exp_refs_by_baseline(repo.scm, rev_set, git_remote)
    for ref_info_list in ref_info_dict.values():
        for ref_info in ref_info_list:
            commit_ref_dict[ref_info.name] = ref_info
    return commit_ref_dict


def _remove_commited_exps(
    scm: "Git", exp_refs_list: Iterable["ExpRefInfo"], remote: Optional[str]
) -> list[str]:
    if remote:
        from dvc.scm import TqdmGit

        for ref_info in exp_refs_list:
            with TqdmGit(desc="Pushing git refs") as pbar:
                push_refspec(
                    scm,
                    remote,
                    [(None, str(ref_info))],
                    progress=pbar.update_git,
                )
    else:
        from .utils import remove_exp_refs

        remove_exp_refs(scm, exp_refs_list)
    return [exp_ref.name for exp_ref in exp_refs_list]
