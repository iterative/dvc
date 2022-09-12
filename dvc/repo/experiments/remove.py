import logging
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Union,
)

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import iter_revs

from .exceptions import UnresolvedExpNamesError
from .refs import ExpRefInfo
from .utils import exp_refs, exp_refs_by_baseline, push_refspec

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.repo.experiments.queue.celery import LocalCeleryQueue
    from dvc.scm import Git

    from .queue.base import QueueEntry


logger = logging.getLogger(__name__)


class ExpRefAndQueueEntry(NamedTuple):
    exp_ref_list: List["ExpRefInfo"]
    queue_entry_list: List["QueueEntry"]
    removed: List["str"]


def _get_ref_and_entry_by_names(
    exp_names: Union[str, List[str]],
    scm: "Git",
    celery_queue: "LocalCeleryQueue",
    git_remote: Optional[str],
) -> ExpRefAndQueueEntry:
    from .utils import resolve_name

    exp_ref_list: List["ExpRefInfo"] = []
    queue_entry_list: List["QueueEntry"] = []
    removed: List[str] = []
    if isinstance(exp_names, str):
        exp_names = [exp_names]
    exp_ref_match: Dict[str, Optional["ExpRefInfo"]] = resolve_name(
        scm, exp_names, git_remote
    )
    if not git_remote:
        queue_entry_match: Dict[
            str, Optional["QueueEntry"]
        ] = celery_queue.match_queue_entry_by_name(
            exp_names, celery_queue.iter_queued(), celery_queue.iter_done()
        )

    remained = []
    for exp_name in exp_names:
        exp_ref = exp_ref_match[exp_name]
        queue_entry = None if git_remote else queue_entry_match[exp_name]
        if exp_ref or queue_entry:
            if exp_ref:
                exp_ref_list.append(exp_ref)
            if queue_entry:
                queue_entry_list.append(queue_entry)
            removed.append(exp_name)
        else:
            remained.append(exp_name)
    if remained:
        raise UnresolvedExpNamesError(remained)
    return ExpRefAndQueueEntry(exp_ref_list, queue_entry_list, removed)


@locked
@scm_context
def remove(
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
        result = _get_ref_and_entry_by_names(
            exp_names, repo.scm, celery_queue, git_remote
        )
        removed.extend(result.removed)
        exp_ref_list.extend(result.exp_ref_list)
        queue_entry_list.extend(result.queue_entry_list)
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
    scm: "Git", exp_refs_list: Iterable[ExpRefInfo], remote: Optional[str]
) -> List[str]:
    if remote:
        from dvc.scm import TqdmGit

        for ref_info in exp_refs_list:
            with TqdmGit(desc="Pushing git refs") as pbar:
                push_refspec(
                    scm,
                    remote,
                    None,
                    str(ref_info),
                    progress=pbar.update_git,
                )
    else:
        from .utils import remove_exp_refs

        remove_exp_refs(scm, exp_refs_list)
    return [exp_ref.name for exp_ref in exp_refs_list]
