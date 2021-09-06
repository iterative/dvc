import logging
from typing import List, Optional

from dvc.exceptions import InvalidArgumentError
from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm.base import RevError

from .utils import exp_refs, remove_exp_refs, resolve_exp_ref

logger = logging.getLogger(__name__)


@locked
@scm_context
def remove(
    repo,
    exp_names=None,
    queue=False,
    clear_all=False,
    remote=None,
    **kwargs,
):
    if not any([exp_names, queue, clear_all]):
        return 0

    removed = 0
    if queue:
        removed += _clear_stash(repo)
    if clear_all:
        removed += _clear_all(repo)

    if exp_names:
        removed += _remove_exp_by_names(repo, remote, exp_names)
    return removed


def _clear_stash(repo):
    removed = len(repo.experiments.stash)
    repo.experiments.stash.clear()
    return removed


def _clear_all(repo):
    ref_infos = list(exp_refs(repo.scm))
    remove_exp_refs(repo.scm, ref_infos)
    return len(ref_infos)


def _get_exp_stash_index(repo, ref_or_rev: str) -> Optional[int]:
    stash_revs = repo.experiments.stash_revs
    for _, ref_info in stash_revs.items():
        if ref_info.name == ref_or_rev:
            return ref_info.index
    try:
        rev = repo.scm.resolve_rev(ref_or_rev)
        if rev in stash_revs:
            return stash_revs.get(rev).index
    except RevError:
        pass
    return None


def _remove_commited_exps(
    repo, remote: Optional[str], exp_names: List[str]
) -> List[str]:
    remain_list = []
    remove_list = []
    for exp_name in exp_names:
        ref_info = resolve_exp_ref(repo.scm, exp_name, remote)

        if ref_info:
            remove_list.append(ref_info)
        else:
            remain_list.append(exp_name)
    if remove_list:
        if not remote:
            remove_exp_refs(repo.scm, remove_list)
        else:
            for ref_info in remove_list:
                repo.scm.push_refspec(remote, None, str(ref_info))
    return remain_list


def _remove_queued_exps(repo, refs_or_revs: List[str]) -> List[str]:
    remain_list = []
    for ref_or_rev in refs_or_revs:
        stash_index = _get_exp_stash_index(repo, ref_or_rev)
        if stash_index is None:
            remain_list.append(ref_or_rev)
        else:
            repo.experiments.stash.drop(stash_index)
    return remain_list


def _remove_exp_by_names(repo, remote, exp_names: List[str]) -> int:
    remained = _remove_commited_exps(repo, remote, exp_names)
    if not remote:
        remained = _remove_queued_exps(repo, remained)
    if remained:
        raise InvalidArgumentError(
            "'{}' is not a valid experiment".format(";".join(remained))
        )
    return len(exp_names) - len(remained)
