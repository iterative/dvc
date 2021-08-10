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
def remove(repo, refs_or_revs=None, queue=False, **kwargs):
    if not refs_or_revs and not queue:
        return 0

    removed = 0
    if queue:
        removed += len(repo.experiments.stash)
        repo.experiments.stash.clear()
    if refs_or_revs:
        for ref_or_rev in refs_or_revs:
            _remove_exp_by_ref_or_rev(repo, ref_or_rev)
            removed += 1
    return removed


def _get_exp_stash_index(repo, ref_or_rev: str) -> Optional[int]:
    stash_ref_infos = repo.experiments.stash_revs
    print("*" * 100)
    for rev, ref_info in stash_ref_infos.items():
        print(rev, ref_info)
        if ref_info.name == ref_or_rev:
            return ref_info.index
        if rev == ref_or_rev:
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


def _remove_exp_by_ref_or_rev(repo, ref_or_rev: str):
    ref_info = _get_exp_ref(repo, ref_or_rev)
    if ref_info is not None:
        remove_exp_refs(repo.scm, [ref_info])
    else:
        stash_index = _get_exp_stash_index(repo, ref_or_rev)
        if stash_index is None:
            raise InvalidArgumentError(
                f"'{ref_or_rev}' is neither a valid experiment reference"
                " nor a queued experiment revision"
            )
        repo.experiments.stash.drop(stash_index)
