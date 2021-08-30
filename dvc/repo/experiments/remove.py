import logging
from typing import List, Optional

from dvc.exceptions import InvalidArgumentError
from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm.base import RevError

from .base import EXPS_NAMESPACE, ExpRefInfo
from .utils import exp_refs, exp_refs_by_name, remove_exp_refs

logger = logging.getLogger(__name__)


@locked
@scm_context
def remove(
    repo,
    exp_names=None,
    queue=False,
    clear_all=False,
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
        remained = _remove_commited_exps(repo, exp_names)
        remained = _remove_queued_exps(repo, remained)
        if remained:
            raise InvalidArgumentError(
                "'{}' is not a valid experiment".format(";".join(remained))
            )
        removed += len(exp_names) - len(remained)
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


def _get_exp_ref(repo, exp_name: str) -> Optional[ExpRefInfo]:
    cur_rev = repo.scm.get_rev()
    if exp_name.startswith(EXPS_NAMESPACE):
        if repo.scm.get_ref(exp_name):
            return ExpRefInfo.from_ref(exp_name)
    else:
        exp_ref_list = list(exp_refs_by_name(repo.scm, exp_name))
        if exp_ref_list:
            return _get_ref(exp_ref_list, exp_name, cur_rev)
    return None


def _get_ref(ref_infos, name, cur_rev) -> Optional[ExpRefInfo]:
    if len(ref_infos) > 1:
        for info in ref_infos:
            if info.baseline_sha == cur_rev:
                return info
        msg = [
            (
                f"Ambiguous name '{name}' refers to multiple "
                "experiments. Use full refname to remove one of "
                "the following:"
            )
        ]
        msg.extend([f"\t{info}" for info in ref_infos])
        raise InvalidArgumentError("\n".join(msg))
    return ref_infos[0]


def _remove_commited_exps(repo, refs: List[str]) -> List[str]:
    remain_list = []
    remove_list = []
    for ref in refs:
        ref_info = _get_exp_ref(repo, ref)
        if ref_info:
            remove_list.append(ref_info)
        else:
            remain_list.append(ref)
    if remove_list:
        remove_exp_refs(repo.scm, remove_list)
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
