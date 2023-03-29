import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Union,
)

from funcy import compact, group_by
from scmrepo.git.backend.base import SyncStatus

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import Git, TqdmGit, iter_revs
from dvc.ui import ui
from dvc.utils import env2bool

from .exceptions import UnresolvedExpNamesError
from .refs import ExpRefInfo
from .utils import exp_commits, exp_refs, exp_refs_by_baseline, resolve_name

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


def notify_refs_to_studio(
    repo: "Repo", git_remote: str, **refs: List[str]
) -> Optional[str]:
    config = repo.config["feature"]
    refs = compact(refs)
    if not refs or env2bool("DVC_TEST"):
        return None

    if not (config.get("studio_token") or config["push_exp_to_studio"]):
        logger.debug(
            "Either feature.studio_token or feature.push_exp_to_studio config "
            "needs to be set."
        )
        return None

    import os

    token = os.environ.get("STUDIO_TOKEN") or config.get("studio_token")
    if not token:
        logger.debug("Studio token not found.")
        return None

    from dulwich.porcelain import get_remote_repo

    from dvc.utils import studio

    _, repo_url = get_remote_repo(repo.scm.dulwich.repo, git_remote)
    studio_url = config.get("studio_url")
    d = studio.notify_refs(repo_url, token, studio_url=studio_url, **refs)
    return d.get("url")


@locked
@scm_context
def push(  # noqa: C901
    repo: "Repo",
    git_remote: str,
    exp_names: Union[Iterable[str], str],
    all_commits: bool = False,
    rev: Optional[str] = None,
    num: int = 1,
    force: bool = False,
    push_cache: bool = False,
    **kwargs: Any,
) -> Dict[str, Any]:
    exp_ref_set: Set["ExpRefInfo"] = set()
    assert isinstance(repo.scm, Git)
    if all_commits:
        exp_ref_set.update(exp_refs(repo.scm))

    else:
        if exp_names:
            if isinstance(exp_names, str):
                exp_names = [exp_names]
            exp_ref_dict = resolve_name(repo.scm, exp_names)

            unresolved_exp_names = []
            for exp_name, exp_ref in exp_ref_dict.items():
                if exp_ref is None:
                    unresolved_exp_names.append(exp_name)
                else:
                    exp_ref_set.add(exp_ref)

            if unresolved_exp_names:
                raise UnresolvedExpNamesError(unresolved_exp_names)

        if rev:
            rev_dict = iter_revs(repo.scm, [rev], num)
            rev_set = set(rev_dict.keys())
            ref_info_dict = exp_refs_by_baseline(repo.scm, rev_set)
            for _, ref_info_list in ref_info_dict.items():
                exp_ref_set.update(ref_info_list)

    push_result = _push(repo, git_remote, exp_ref_set, force)
    if push_result[SyncStatus.DIVERGED]:
        diverged_refs = [ref.name for ref in push_result[SyncStatus.DIVERGED]]
        ui.warn(
            f"Local experiment '{diverged_refs}' has diverged from remote "
            "experiment with the same name. To override the remote experiment "
            "re-run with '--force'."
        )
    if push_cache:
        push_cache_ref = (
            push_result[SyncStatus.UP_TO_DATE] + push_result[SyncStatus.SUCCESS]
        )
        _push_cache(repo, push_cache_ref, **kwargs)

    refs = push_result[SyncStatus.SUCCESS]
    pushed_refs = [str(r) for r in refs]
    url = notify_refs_to_studio(repo, git_remote, pushed=pushed_refs)
    return {"pushed": [ref.name for ref in refs], "url": url}


def _push(
    repo: "Repo",
    git_remote: str,
    refs: Iterable["ExpRefInfo"],
    force: bool,
) -> Mapping[SyncStatus, List["ExpRefInfo"]]:
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

    pull_result: Mapping[SyncStatus, List["ExpRefInfo"]] = group_by(group_result, refs)

    return pull_result


def _push_cache(
    repo: "Repo",
    refs: Union[ExpRefInfo, Iterable["ExpRefInfo"]],
    dvc_remote: Optional[str] = None,
    jobs: Optional[int] = None,
    run_cache: bool = False,
):
    if isinstance(refs, ExpRefInfo):
        refs = [refs]
    assert isinstance(repo.scm, Git)
    revs = list(exp_commits(repo.scm, refs))
    logger.debug("dvc push experiment '%s'", refs)
    repo.push(jobs=jobs, remote=dvc_remote, run_cache=run_cache, revs=revs)
