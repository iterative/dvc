import os
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urljoin

import requests
from funcy import compact, ignore
from requests.adapters import HTTPAdapter

from dvc.env import (
    DVC_STUDIO_OFFLINE,
    DVC_STUDIO_REPO_URL,
    DVC_STUDIO_TOKEN,
    DVC_STUDIO_URL,
)
from dvc.log import logger
from dvc.utils import as_posix

if TYPE_CHECKING:
    from requests import Response

    from dvc.repo import Repo


logger = logger.getChild(__name__)

STUDIO_URL = "https://studio.dvc.ai"


def post(
    url: str,
    token: str,
    data: dict[str, Any],
    base_url: Optional[str] = STUDIO_URL,
    max_retries: int = 3,
    timeout: int = 5,
) -> "Response":
    url = urljoin(base_url or STUDIO_URL, url)
    session = requests.Session()
    session.mount(url, HTTPAdapter(max_retries=max_retries))

    logger.trace("Sending %s to %s", data, url)

    headers = {"Authorization": f"token {token}"}
    r = session.post(
        url, json=data, headers=headers, timeout=timeout, allow_redirects=False
    )
    r.raise_for_status()
    return r


def notify_refs(
    repo_url: str,
    token: str,
    *,
    base_url: Optional[str] = STUDIO_URL,
    **refs: list[str],
) -> dict[str, Any]:
    extra_keys = refs.keys() - {"pushed", "removed"}
    assert not extra_keys, f"got extra args: {extra_keys}"

    refs = compact(refs)
    if not refs:
        return {}

    logger.debug(
        "notifying Studio%s about updated experiments",
        f" ({base_url})" if base_url else "",
    )
    data = {"repo_url": repo_url, "client": "dvc", "refs": refs}

    try:
        r = post("webhook/dvc", token, data, base_url=base_url)
    except requests.RequestException as e:
        logger.trace("", exc_info=True)

        msg = str(e)
        if e.response is None:
            logger.warning("failed to notify Studio: %s", msg.lower())
            return {}

        r = e.response
        d = ignore(Exception, default={})(r.json)()
        status = r.status_code
        if detail := d.get("detail"):
            msg = f"{detail} ({status=})"
        logger.warning("failed to notify Studio: %s", msg.lower())
    else:
        d = r.json()

    if d:
        logger.trace("received response: %s (status=%r)", d, r.status_code)
    return d


def config_to_env(config: dict[str, Any]) -> dict[str, Any]:
    env = {}
    if "offline" in config:
        env[DVC_STUDIO_OFFLINE] = config["offline"]
    if "repo_url" in config:
        env[DVC_STUDIO_REPO_URL] = config["repo_url"]
    if "token" in config:
        env[DVC_STUDIO_TOKEN] = config["token"]
    if "url" in config:
        env[DVC_STUDIO_URL] = config["url"]
    return env


def env_to_config(env: dict[str, Any]) -> dict[str, Any]:
    config = {}
    if DVC_STUDIO_OFFLINE in env:
        config["offline"] = env[DVC_STUDIO_OFFLINE]
    if DVC_STUDIO_REPO_URL in env:
        config["repo_url"] = env[DVC_STUDIO_REPO_URL]
    if DVC_STUDIO_TOKEN in env:
        config["token"] = env[DVC_STUDIO_TOKEN]
    if DVC_STUDIO_URL in env:
        config["url"] = env[DVC_STUDIO_URL]
    return config


def get_subrepo_relpath(repo: "Repo") -> str:
    from dvc.fs import GitFileSystem

    scm_root_dir = "/" if isinstance(repo.fs, GitFileSystem) else repo.scm.root_dir

    relpath = as_posix(repo.fs.relpath(repo.root_dir, scm_root_dir))

    return "" if relpath == "." else relpath


def get_repo_url(repo: "Repo") -> str:
    from dulwich.porcelain import get_remote_repo

    from dvc.env import DVC_EXP_GIT_REMOTE

    repo_url = os.getenv(
        DVC_EXP_GIT_REMOTE, repo.config.get("exp", {}).get("git_remote")
    )
    if repo_url:
        try:
            _, repo_url = get_remote_repo(repo.scm.dulwich.repo, repo_url)
        except IndexError:
            pass
    return repo_url
