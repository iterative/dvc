import logging
from typing import Iterable, List, Mapping, Optional, Set, Union

from funcy import group_by
from scmrepo.git.backend.base import SyncStatus

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import TqdmGit, iter_revs
from dvc.ui import ui

from .exceptions import UnresolvedExpNamesError
from .refs import ExpRefInfo
from .utils import exp_commits, exp_refs, exp_refs_by_baseline, resolve_name

logger = logging.getLogger(__name__)


STUDIO_URL = "https://studio.iterative.ai"


@locked
@scm_context
def push(  # noqa: C901, PLR0912
    repo,
    git_remote: str,
    exp_names: Union[Iterable[str], str],
    all_commits=False,
    rev: Optional[str] = None,
    num=1,
    force: bool = False,
    push_cache: bool = False,
    **kwargs,
) -> Iterable[str]:
    from dvc.utils import env2bool

    exp_ref_set: Set["ExpRefInfo"] = set()
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
    feature_config = repo.config["feature"]

    push_to_studio = (
        bool(feature_config.get("studio_token")) or feature_config["push_exp_to_studio"]
    )
    if refs and push_to_studio and not env2bool("DVC_TEST"):
        token, repo_url = get_studio_token_and_repo_url(feature_config)
        if token and repo_url:
            studio_url = feature_config.get("studio_url")
            _notify_studio([str(ref) for ref in refs], repo_url, token, url=studio_url)
    return [ref.name for ref in refs]


def get_studio_token_and_repo_url(config):
    import os

    from dvc_studio_client.post_live_metrics import get_studio_repo_url

    token = os.getenv("STUDIO_TOKEN") or config.get("studio_token")
    if not token:
        logger.debug("Studio token not found. Skipping push to Studio.")
    repo_url = os.getenv("STUDIO_REPO_URL") or get_studio_repo_url()
    if token and not repo_url:
        logger.warning(
            "Could not detect repository url. "
            "Please set STUDIO_REPO_URL environment variable "
            "to your remote git repository url. "
        )
    return token, repo_url


def _notify_studio(
    refs: List[str],
    repo_url: str,
    token: str,
    url: Optional[str] = None,
):
    if not refs:
        return

    from urllib.parse import urljoin

    import requests
    from requests.adapters import HTTPAdapter

    endpoint = urljoin(url or STUDIO_URL, "/webhook/dvc")
    session = requests.Session()
    session.mount(endpoint, HTTPAdapter(max_retries=3))

    logger.debug("pushing experiments to Studio (%s)", url)
    json = {"repo_url": repo_url, "client": "dvc", "refs": refs}
    logger.trace("Sending %s to %s", json, endpoint)  # type: ignore[attr-defined]

    headers = {"Authorization": f"token {token}"}
    resp = session.post(endpoint, json=json, headers=headers, timeout=5)
    resp.raise_for_status()


def _push(
    repo,
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
    repo,
    refs: Union[ExpRefInfo, Iterable["ExpRefInfo"]],
    dvc_remote=None,
    jobs=None,
    run_cache=False,
):
    if isinstance(refs, ExpRefInfo):
        refs = [refs]
    revs = list(exp_commits(repo.scm, refs))
    logger.debug("dvc push experiment '%s'", refs)
    repo.push(jobs=jobs, remote=dvc_remote, run_cache=run_cache, revs=revs)
