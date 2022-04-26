from collections import defaultdict
from functools import wraps
from typing import (
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Union,
)

from scmrepo.git import Git

from dvc.exceptions import InvalidArgumentError
from dvc.repo.experiments.exceptions import AmbiguousExpRefInfo

from .refs import (
    EXEC_APPLY,
    EXEC_BASELINE,
    EXEC_BRANCH,
    EXEC_CHECKPOINT,
    EXEC_NAMESPACE,
    EXPS_NAMESPACE,
    STASHES,
    ExpRefInfo,
)


def scm_locked(f):
    # Lock the experiments workspace so that we don't try to perform two
    # different sequences of git operations at once
    @wraps(f)
    def wrapper(exp, *args, **kwargs):
        from dvc.scm import map_scm_exception

        with map_scm_exception(), exp.scm_lock:
            return f(exp, *args, **kwargs)

    return wrapper


def unlocked_repo(f):
    @wraps(f)
    def wrapper(exp, *args, **kwargs):
        exp.repo.lock.unlock()
        exp.repo._reset()  # pylint: disable=protected-access
        try:
            ret = f(exp, *args, **kwargs)
        finally:
            exp.repo.lock.lock()
        return ret

    return wrapper


def exp_refs(
    scm: "Git", url: Optional[str] = None
) -> Generator["ExpRefInfo", None, None]:
    """Iterate over all experiment refs."""
    ref_gen = (
        iter_remote_refs(scm, url, base=EXPS_NAMESPACE)
        if url
        else scm.iter_refs(base=EXPS_NAMESPACE)
    )
    for ref in ref_gen:
        if ref.startswith(EXEC_NAMESPACE) or ref in STASHES:
            continue
        yield ExpRefInfo.from_ref(ref)


def exp_refs_by_rev(scm: "Git", rev: str) -> Generator[ExpRefInfo, None, None]:
    """Iterate over all experiment refs pointing to the specified revision."""
    for ref in scm.get_refs_containing(rev, EXPS_NAMESPACE):
        if not (ref.startswith(EXEC_NAMESPACE) or ref in STASHES):
            yield ExpRefInfo.from_ref(ref)


def exp_refs_by_baseline(
    scm: "Git", revs: Set[str], url: Optional[str] = None
) -> Mapping[str, List[ExpRefInfo]]:
    """Iterate over all experiment refs with the specified baseline."""
    all_exp_refs = exp_refs(scm, url)
    result = defaultdict(list)
    for ref in all_exp_refs:
        if ref.baseline_sha in revs:
            result[ref.baseline_sha].append(ref)
    return result


def iter_remote_refs(
    scm: "Git", url: str, base: Optional[str] = None, **kwargs
):
    from scmrepo.exceptions import AuthError, InvalidRemote

    from dvc.scm import GitAuthError, InvalidRemoteSCMRepo

    try:
        yield from scm.iter_remote_refs(url, base=base, **kwargs)
    except InvalidRemote as exc:
        raise InvalidRemoteSCMRepo(str(exc))
    except AuthError as exc:
        raise GitAuthError(str(exc))


def push_refspec(
    scm: "Git",
    url: str,
    src: Optional[str],
    dest: str,
    force: bool = False,
    on_diverged: Optional[Callable[[str, str], bool]] = None,
    **kwargs,
):
    from scmrepo.exceptions import AuthError
    from scmrepo.git.backend.base import SyncStatus

    from ...scm import GitAuthError, SCMError

    if not src:
        refspecs = [f":{dest}"]
    elif src.endswith("/"):
        refspecs = []
        dest = dest.rstrip("/") + "/"
        for ref in scm.iter_refs(base=src):
            refname = ref.split("/")[-1]
            refspecs.append(f"{ref}:{dest}{refname}")
    else:
        if dest.endswith("/"):
            refname = src.split("/")[-1]
            refspecs = [f"{src}:{dest}/{refname}"]
        else:
            refspecs = [f"{src}:{dest}"]

    try:
        results = scm.push_refspecs(
            url, refspecs, force=force, on_diverged=on_diverged, **kwargs
        )
        diverged = [
            ref for ref in results if results[ref] == SyncStatus.DIVERGED
        ]

        if diverged:
            raise SCMError(
                f"local ref '{diverged}' diverged from remote '{url}'"
            )
    except AuthError as exc:
        raise GitAuthError(str(exc))


def remote_exp_refs(scm: "Git", url: str) -> Generator[ExpRefInfo, None, None]:
    """Iterate over all remote experiment refs."""
    for ref in iter_remote_refs(scm, url, base=EXPS_NAMESPACE):
        if ref.startswith(EXEC_NAMESPACE) or ref in STASHES:
            continue
        yield ExpRefInfo.from_ref(ref)


def exp_refs_by_names(
    scm: "Git", names: Set[str], url: Optional[str] = None
) -> Dict[str, List[ExpRefInfo]]:
    """Iterate over all experiment refs matching the specified names."""
    resolve_results = defaultdict(list)
    ref_info_gen = exp_refs(scm, url)
    for ref_info in ref_info_gen:
        if ref_info.name in names:
            resolve_results[ref_info.name].append(ref_info)

    return resolve_results


def remote_exp_refs_by_baseline(
    scm: "Git", url: str, rev: str
) -> Generator[ExpRefInfo, None, None]:
    """Iterate over all remote experiment refs with the specified baseline."""
    ref_info = ExpRefInfo(baseline_sha=rev)
    for ref in iter_remote_refs(scm, url, base=str(ref_info)):
        if ref.startswith(EXEC_NAMESPACE) or ref in STASHES:
            continue
        yield ExpRefInfo.from_ref(ref)


def exp_commits(
    scm: "Git", ref_infos: Iterable[ExpRefInfo] = None
) -> Iterable[str]:
    """Iterate over all experiment commits."""
    shas: Set["str"] = set()
    refs = ref_infos if ref_infos else exp_refs(scm)
    for ref_info in refs:
        shas.update(scm.branch_revs(str(ref_info), ref_info.baseline_sha))
        if ref_info.baseline_sha:
            shas.add(ref_info.baseline_sha)
    yield from shas


def remove_exp_refs(scm: "Git", ref_infos: Iterable[ExpRefInfo]):
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


def resolve_name(
    scm: "Git",
    exp_names: Union[Iterable[str], str],
    git_remote: Optional[str] = None,
) -> Dict[str, Optional[ExpRefInfo]]:
    """find the ref_info of specified names."""
    if isinstance(exp_names, str):
        exp_names = [exp_names]

    result = {}
    unresolved = set()
    for exp_name in exp_names:
        if exp_name.startswith("refs/"):
            result[exp_name] = ExpRefInfo.from_ref(exp_name)
        else:
            unresolved.add(exp_name)

    unresolved_result = exp_refs_by_names(scm, unresolved, git_remote)
    cur_rev = scm.get_rev()
    for name in unresolved:
        ref_info_list = unresolved_result[name]
        if not ref_info_list:
            result[name] = None
        elif len(ref_info_list) == 1:
            result[name] = ref_info_list[0]
        else:
            for ref_info in ref_info_list:
                if ref_info.baseline_sha == cur_rev:
                    result[name] = ref_info
                    break
            else:
                raise AmbiguousExpRefInfo(name, ref_info_list)
    return result


def check_ref_format(scm: "Git", ref: ExpRefInfo):
    # "/" forbidden, only in dvc exp as we didn't support it for now.
    if not scm.check_ref_format(str(ref)) or "/" in ref.name:
        raise InvalidArgumentError(
            f"Invalid exp name {ref.name}, the exp name must follow rules in "
            "https://git-scm.com/docs/git-check-ref-format"
        )


def fetch_all_exps(scm: "Git", url: str, progress: Optional[Callable] = None):
    refspecs = [
        f"{ref}:{ref}"
        for ref in iter_remote_refs(scm, url, base=EXPS_NAMESPACE)
        if not (ref.startswith(EXEC_NAMESPACE) or ref in STASHES)
    ]
    scm.fetch_refspecs(
        url,
        refspecs,
        progress=progress,
    )
