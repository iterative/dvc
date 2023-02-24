import logging
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Union

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


logger = logging.getLogger(__name__)


@locked
@scm_context
def remove(  # noqa: C901
    repo: "Repo",
    exp_names: Union[None, str, List[str]] = None,
    rev: Optional[str] = None,
    all_commits: bool = False,
    num: int = 1,
    queue: bool = False,
    git_remote: Optional[str] = None,
) -> List[str]:
    removed: List[str] = []
    if not any([exp_names, queue, all_commits, rev]):
        return removed
    celery_queue: "LocalCeleryQueue" = repo.experiments.celery_queue

    if queue:
        removed.extend(celery_queue.clear(queued=True))

    assert isinstance(repo.scm, Git)
    if all_commits:
        removed.extend(
            _remove_commited_exps(
                repo.scm, list(exp_refs(repo.scm, git_remote)), git_remote
            )
        )
        return removed

    exp_ref_list: List["ExpRefInfo"] = []
    queue_entry_list: List["QueueEntry"] = []
    if exp_names:
        results: Dict[
            str, "ExpRefAndQueueEntry"
        ] = celery_queue.get_ref_and_entry_by_names(exp_names, git_remote)
        remained: List[str] = []
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
            raise UnresolvedExpNamesError(remained)
    elif rev:
        exp_ref_dict = _resolve_exp_by_baseline(repo, rev, num, git_remote)
        removed.extend(exp_ref_dict.keys())
        exp_ref_list.extend(exp_ref_dict.values())

    if exp_ref_list:
        _remove_commited_exps(repo.scm, exp_ref_list, git_remote)

    if queue_entry_list:
        from .queue.remove import remove_tasks

        remove_tasks(celery_queue, queue_entry_list)

    return removed


def _resolve_exp_by_baseline(
    repo,
    rev: str,
    num: int,
    git_remote: Optional[str] = None,
) -> Dict[str, "ExpRefInfo"]:
    commit_ref_dict: Dict[str, "ExpRefInfo"] = {}
    rev_dict = iter_revs(repo.scm, [rev], num)
    rev_set = set(rev_dict.keys())
    ref_info_dict = exp_refs_by_baseline(repo.scm, rev_set, git_remote)
    for _, ref_info_list in ref_info_dict.items():
        for ref_info in ref_info_list:
            commit_ref_dict[ref_info.name] = ref_info
    return commit_ref_dict


def _remove_commited_exps(
    scm: "Git", exp_refs_list: Iterable["ExpRefInfo"], remote: Optional[str]
) -> List[str]:
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
