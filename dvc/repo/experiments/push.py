from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Optional, Union

from funcy import compact, group_by
from scmrepo.git.backend.base import SyncStatus

from dvc.env import DVC_STUDIO_TOKEN, DVC_STUDIO_URL
from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import Git, TqdmGit, iter_revs
from dvc.utils import env2bool
from dvc.utils.collections import ensure_list

from .exceptions import UnresolvedExpNamesError
from .refs import ExpRefInfo
from .utils import exp_commits, exp_refs, exp_refs_by_baseline, resolve_name

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logger.getChild(__name__)


class UploadError(DvcException):
    def __init__(self, msg, result):
        self.result = result
        super().__init__(msg)


def notify_refs_to_studio(
    repo: "Repo", git_remote: str, **refs: list[str]
) -> Optional[str]:
    import os

    config = repo.config["studio"]
    refs = compact(refs)
    if not refs or env2bool("DVC_TEST"):
        return None

    token = (
        os.environ.get(DVC_STUDIO_TOKEN)
        or os.environ.get("STUDIO_TOKEN")
        or config.get("token")
    )
    if not token:
        logger.debug("Studio token not found.")
        return None

    from dulwich.porcelain import get_remote_repo

    from dvc.utils import studio

    _, repo_url = get_remote_repo(repo.scm.dulwich.repo, git_remote)
    studio_url = os.environ.get(DVC_STUDIO_URL) or config.get("url")
    d = studio.notify_refs(repo_url, token, base_url=studio_url, **refs)
    return d.get("url")


def exp_refs_from_names(scm: "Git", exp_names: list[str]) -> set["ExpRefInfo"]:
    exp_ref_set = set()
    exp_ref_dict = resolve_name(scm, exp_names)
    unresolved_exp_names = []
    for exp_name, exp_ref in exp_ref_dict.items():
        if exp_ref is None:
            unresolved_exp_names.append(exp_name)
        else:
            exp_ref_set.add(exp_ref)

    if unresolved_exp_names:
        raise UnresolvedExpNamesError(unresolved_exp_names)
    return exp_ref_set


def exp_refs_from_rev(scm: "Git", rev: list[str], num: int = 1) -> set["ExpRefInfo"]:
    exp_ref_set = set()
    rev_dict = iter_revs(scm, rev, num)
    rev_set = set(rev_dict.keys())
    ref_info_dict = exp_refs_by_baseline(scm, rev_set)
    for _, ref_info_list in ref_info_dict.items():
        exp_ref_set.update(ref_info_list)
    return exp_ref_set


@locked
@scm_context
def push(
    repo: "Repo",
    git_remote: str,
    exp_names: Optional[Union[list[str], str]] = None,
    all_commits: bool = False,
    rev: Optional[Union[list[str], str]] = None,
    num: int = 1,
    force: bool = False,
    push_cache: bool = False,
    **kwargs: Any,
) -> dict[str, Any]:
    exp_ref_set: set[ExpRefInfo] = set()
    assert isinstance(repo.scm, Git)
    if all_commits:
        exp_ref_set.update(exp_refs(repo.scm))
    if exp_names:
        exp_ref_set.update(exp_refs_from_names(repo.scm, ensure_list(exp_names)))
    else:
        rev = rev or "HEAD"
        if isinstance(rev, str):
            rev = [rev]
        exp_ref_set.update(exp_refs_from_rev(repo.scm, rev, num=num))

    push_result = _push(repo, git_remote, exp_ref_set, force)

    refs = {
        status.name.lower(): [ref.name for ref in ref_list]
        for status, ref_list in push_result.items()
    }
    result: dict[str, Any] = {**refs, "uploaded": 0}

    pushed_refs_info = (
        push_result[SyncStatus.UP_TO_DATE] + push_result[SyncStatus.SUCCESS]
    )

    e = None
    if push_cache:
        try:
            result["uploaded"] = _push_cache(repo, pushed_refs_info, **kwargs)
        except Exception as exc:  # noqa: BLE001
            e = exc

    pushed_refs = [str(r) for r in pushed_refs_info]
    result["url"] = notify_refs_to_studio(repo, git_remote, pushed=pushed_refs)

    if e:
        raise UploadError("failed to push cache", result) from e
    return result


def _push(
    repo: "Repo",
    git_remote: str,
    refs: Iterable["ExpRefInfo"],
    force: bool,
) -> Mapping[SyncStatus, list["ExpRefInfo"]]:
    from scmrepo.exceptions import AuthError

    from dvc.scm import GitAuthError

    refspec_list = [f"{exp_ref}:{exp_ref}" for exp_ref in refs]
    logger.debug("git push experiment %s -> '%s'", refspec_list, git_remote)

    with TqdmGit(desc="Pushing git refs") as pbar:
        try:
            results: Mapping[str, SyncStatus] = repo.scm.push_refspecs(
                git_remote,
                refspec_list,
                force=force,
                progress=pbar.update_git,
            )
        except AuthError as exc:
            raise GitAuthError(str(exc))  # noqa: B904

    def group_result(refspec):
        return results[str(refspec)]

    pull_result: Mapping[SyncStatus, list[ExpRefInfo]] = group_by(group_result, refs)

    return pull_result


def _push_cache(
    repo: "Repo",
    refs: Union[ExpRefInfo, Iterable["ExpRefInfo"]],
    dvc_remote: Optional[str] = None,
    jobs: Optional[int] = None,
    run_cache: bool = False,
) -> int:
    if isinstance(refs, ExpRefInfo):
        refs = [refs]
    assert isinstance(repo.scm, Git)
    revs = list(exp_commits(repo.scm, refs))
    logger.debug("dvc push experiment '%s'", refs)
    return repo.push(
        jobs=jobs, remote=dvc_remote, run_cache=run_cache, revs=revs, workspace=False
    )
