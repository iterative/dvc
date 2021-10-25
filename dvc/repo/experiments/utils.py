from typing import Generator, Iterable, Optional, Set

from dvc.exceptions import InvalidArgumentError
from dvc.scm.git import Git

from .base import (
    EXEC_BASELINE,
    EXEC_NAMESPACE,
    EXPS_NAMESPACE,
    EXPS_STASH,
    ExpRefInfo,
)


def exp_refs(scm: "Git") -> Generator["ExpRefInfo", None, None]:
    """Iterate over all experiment refs."""
    for ref in scm.iter_refs(base=EXPS_NAMESPACE):
        if ref.startswith(EXEC_NAMESPACE) or ref == EXPS_STASH:
            continue
        yield ExpRefInfo.from_ref(ref)


def exp_refs_by_rev(
    scm: "Git", rev: str
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all experiment refs pointing to the specified revision."""
    for ref in scm.get_refs_containing(rev, EXPS_NAMESPACE):
        if not (ref.startswith(EXEC_NAMESPACE) or ref == EXPS_STASH):
            yield ExpRefInfo.from_ref(ref)


def exp_refs_by_name(
    scm: "Git", name: str
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all experiment refs matching the specified name."""
    for ref_info in exp_refs(scm):
        if ref_info.name == name:
            yield ref_info


def exp_refs_by_baseline(
    scm: "Git", rev: str
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all experiment refs with the specified baseline."""
    ref_info = ExpRefInfo(baseline_sha=rev)
    for ref in scm.iter_refs(base=str(ref_info)):
        if ref.startswith(EXEC_NAMESPACE) or ref == EXPS_STASH:
            continue
        yield ExpRefInfo.from_ref(ref)


def remote_exp_refs(
    scm: "Git", url: str
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all remote experiment refs."""
    for ref in scm.iter_remote_refs(url, base=EXPS_NAMESPACE):
        if ref.startswith(EXEC_NAMESPACE) or ref == EXPS_STASH:
            continue
        yield ExpRefInfo.from_ref(ref)


def remote_exp_refs_by_name(
    scm: "Git", url: str, name: str
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all remote experiment refs matching the specified name."""
    for ref_info in remote_exp_refs(scm, url):
        if ref_info.name == name:
            yield ref_info


def remote_exp_refs_by_baseline(
    scm: "Git", url: str, rev: str
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all remote experiment refs with the specified baseline."""
    ref_info = ExpRefInfo(baseline_sha=rev)
    for ref in scm.iter_remote_refs(url, base=str(ref_info)):
        if ref.startswith(EXEC_NAMESPACE) or ref == EXPS_STASH:
            continue
        yield ExpRefInfo.from_ref(ref)


def exp_commits(
    scm: "Git", ref_infos: Iterable["ExpRefInfo"] = None
) -> Iterable[str]:
    """Iterate over all experiment commits."""
    shas: Set["str"] = set()
    refs = ref_infos if ref_infos else exp_refs(scm)
    for ref_info in refs:
        shas.update(scm.branch_revs(str(ref_info), ref_info.baseline_sha))
        if ref_info.baseline_sha:
            shas.add(ref_info.baseline_sha)
    yield from shas


def remove_exp_refs(scm: "Git", ref_infos: Iterable["ExpRefInfo"]):
    from .base import EXEC_APPLY, EXEC_BRANCH, EXEC_CHECKPOINT

    exec_branch = scm.get_ref(EXEC_BRANCH, follow=False)
    exec_apply = scm.get_ref(EXEC_APPLY)
    exec_checkpoint = scm.get_ref(EXEC_CHECKPOINT)

    for ref_info in ref_infos:
        ref = scm.get_ref(str(ref_info))
        if exec_branch and str(ref_info):
            scm.remove_ref(EXEC_BRANCH)
        if exec_apply and exec_apply == ref:
            scm.remove_ref(EXEC_APPLY)
        if exec_checkpoint and exec_checkpoint == ref:
            scm.remove_ref(EXEC_CHECKPOINT)
        scm.remove_ref(str(ref_info))


def fix_exp_head(scm: "Git", ref: Optional[str]) -> Optional[str]:
    if ref:
        name, tail = Git.split_ref_pattern(ref)
        if name == "HEAD" and scm.get_ref(EXEC_BASELINE):
            return "".join((EXEC_BASELINE, tail))
    return ref


def resolve_exp_ref(
    scm, exp_name: str, git_remote: Optional[str] = None
) -> Optional[ExpRefInfo]:
    if exp_name.startswith("refs/"):
        return ExpRefInfo.from_ref(exp_name)

    if git_remote:
        exp_ref_list = list(remote_exp_refs_by_name(scm, git_remote, exp_name))
    else:
        exp_ref_list = list(exp_refs_by_name(scm, exp_name))

    if not exp_ref_list:
        return None
    if len(exp_ref_list) > 1:
        cur_rev = scm.get_rev()
        for info in exp_ref_list:
            if info.baseline_sha == cur_rev:
                return info
        if git_remote:
            msg = [
                (
                    f"Ambiguous name '{exp_name}' refers to multiple "
                    "experiments. Use full refname to push one of the "
                    "following:"
                ),
                "",
            ]
        else:
            msg = [
                (
                    f"Ambiguous name '{exp_name}' refers to multiple "
                    f"experiments in '{git_remote}'. Use full refname to pull "
                    "one of the following:"
                ),
                "",
            ]
        msg.extend([f"\t{info}" for info in exp_ref_list])
        raise InvalidArgumentError("\n".join(msg))
    return exp_ref_list[0]


def check_ref_format(scm: "Git", ref: ExpRefInfo):
    # "/" forbidden, only in dvc exp as we didn't support it for now.
    if not scm.check_ref_format(str(ref)) or "/" in ref.name:
        raise InvalidArgumentError(
            f"Invalid exp name {ref.name}, the exp name must follow rules in "
            "https://git-scm.com/docs/git-check-ref-format"
        )
