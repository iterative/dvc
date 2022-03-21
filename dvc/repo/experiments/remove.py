import logging
from typing import TYPE_CHECKING, Dict, List, Mapping, Optional, Set, Union

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import iter_revs

from .exceptions import UnresolvedExpNamesError
from .utils import (
    exp_refs,
    exp_refs_by_baseline,
    push_refspec,
    remove_exp_refs,
    resolve_name,
)

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.scm import Git

    from .base import ExpRefInfo


logger = logging.getLogger(__name__)


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

    if queue:
        removed.extend(_clear_stash(repo))
    if all_commits:
        removed.extend(_clear_all_commits(repo, git_remote))
        return removed

    commit_ref_dict: Dict["ExpRefInfo", str] = {}
    queued_ref_dict: Dict[int, str] = {}
    if exp_names:
        _resolve_exp_by_name(
            repo, exp_names, commit_ref_dict, queued_ref_dict, git_remote
        )

    if rev:
        _resolve_exp_by_baseline(repo, rev, num, commit_ref_dict, git_remote)

    if commit_ref_dict:
        removed.extend(
            _remove_commited_exps(repo.scm, commit_ref_dict, git_remote)
        )

    if queued_ref_dict:
        removed.extend(_remove_queued_exps(repo, queued_ref_dict))

    return removed


def _resolve_exp_by_name(
    repo: "Repo",
    exp_names: Union[str, List[str]],
    commit_ref_dict: Dict["ExpRefInfo", str],
    queued_ref_dict: Dict[int, str],
    git_remote: Optional[str],
):
    remained = set()
    if isinstance(exp_names, str):
        exp_names = [exp_names]

    exp_ref_dict = resolve_name(repo.scm, exp_names, git_remote)
    for exp_name, exp_ref in exp_ref_dict.items():
        if exp_ref is None:
            remained.add(exp_name)
        else:
            commit_ref_dict[exp_ref] = exp_name

    if not git_remote:
        stash_index_dict = _get_queued_index_by_names(repo, remained)
        for exp_name, stash_index in stash_index_dict.items():
            if stash_index is not None:
                queued_ref_dict[stash_index] = exp_name
                remained.remove(exp_name)

    if remained:
        raise UnresolvedExpNamesError(remained)


def _resolve_exp_by_baseline(
    repo,
    rev: str,
    num: int,
    commit_ref_dict: Dict["ExpRefInfo", str],
    git_remote: Optional[str] = None,
):
    rev_dict = iter_revs(repo.scm, [rev], num)
    rev_set = set(rev_dict.keys())
    ref_info_dict = exp_refs_by_baseline(repo.scm, rev_set, git_remote)

    for _, ref_info_list in ref_info_dict.items():
        for ref_info in ref_info_list:
            if ref_info not in commit_ref_dict:
                commit_ref_dict[ref_info] = ref_info.name


def _clear_stash(repo: "Repo") -> List[str]:
    removed_name_list = []
    for entry in repo.experiments.stash.list():
        new_sha = entry.new_sha.decode("utf8")
        name = repo.experiments.get_exact_name(new_sha)
        if not name:
            name = new_sha[:7]
        removed_name_list.append(name)
    repo.experiments.stash.clear()
    return removed_name_list


def _clear_all_commits(repo, git_remote) -> List:
    ref_infos = {
        ref_info: ref_info.name for ref_info in exp_refs(repo.scm, git_remote)
    }
    return _remove_commited_exps(repo.scm, ref_infos, git_remote)


def _get_queued_index_by_names(
    repo,
    exp_name_set: Set[str],
) -> Mapping[str, Optional[int]]:
    from scmrepo.exceptions import RevError as InternalRevError

    result = {}
    stash_revs = repo.experiments.stash_revs
    for _, entry in stash_revs.items():
        if entry.name in exp_name_set:
            result[entry.name] = entry.stash_index

    for exp_name in exp_name_set:
        if exp_name in result:
            continue
        try:
            rev = repo.scm.resolve_rev(exp_name)
            if rev in stash_revs:
                result[exp_name] = stash_revs.get(rev).stash_index
        except InternalRevError:
            result[exp_name] = None
    return result


def _remove_commited_exps(
    scm: "Git", exp_ref_dict: Mapping["ExpRefInfo", str], remote: Optional[str]
) -> List[str]:
    if remote:
        from dvc.scm import TqdmGit

        for ref_info in exp_ref_dict:
            with TqdmGit(desc="Pushing git refs") as pbar:
                push_refspec(
                    scm,
                    remote,
                    None,
                    str(ref_info),
                    progress=pbar.update_git,
                )
    else:
        remove_exp_refs(scm, exp_ref_dict)
    return list(exp_ref_dict.values())


def _remove_queued_exps(repo, indexes: Mapping[int, str]) -> List[str]:
    index_list = list(indexes.keys())
    index_list.sort(reverse=True)
    for index in index_list:
        repo.experiments.stash.drop(index)
    return list(indexes.values())
