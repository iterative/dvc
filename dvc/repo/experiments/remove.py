import logging
from typing import Optional

from dvc.exceptions import InvalidArgumentError
from dvc.repo import locked
from dvc.repo.scm_context import scm_context

from .base import EXPS_NAMESPACE, ExpRefInfo
from .utils import exp_refs_by_name, remove_exp_refs

logger = logging.getLogger(__name__)


@locked
@scm_context
def remove(repo, exp_names=None, queue=False, **kwargs):
    if not exp_names and not queue:
        return 0

    removed = 0
    if queue:
        removed += len(repo.experiments.stash)
        repo.experiments.stash.clear()
    if exp_names:
        for exp_name in exp_names:
            _remove_exp_by_name(repo, exp_name)
            removed += 1
    return removed


def _get_exp_stash_index(repo, exp_name: str) -> Optional[int]:
    stash_ref_infos = repo.experiments.stash_revs
    for _, ref_info in stash_ref_infos.items():
        if ref_info.name == exp_name:
            return ref_info.index
    return None


def _get_exp_ref(repo, exp_name: str) -> Optional[ExpRefInfo]:
    cur_rev = repo.scm.get_rev()
    if exp_name.startswith(EXPS_NAMESPACE):
        if repo.scm.get_ref(exp_name):
            return ExpRefInfo.from_ref(exp_name)
    else:
        exp_refs = list(exp_refs_by_name(repo.scm, exp_name))
        if exp_refs:
            return _get_ref(exp_refs, exp_name, cur_rev)
    return None


def _get_ref(ref_infos, name, cur_rev) -> Optional[ExpRefInfo]:
    if len(ref_infos) == 0:
        return None
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


def _remove_exp_by_name(repo, exp_name: str):
    ref_info = _get_exp_ref(repo, exp_name)
    if ref_info is not None:
        remove_exp_refs(repo.scm, [ref_info])
    else:
        stash_index = _get_exp_stash_index(repo, exp_name)
        if stash_index is None:
            raise InvalidArgumentError(
                f"'{exp_name}' is not a valid experiment name"
            )
        repo.experiments.stash.drop(stash_index)
