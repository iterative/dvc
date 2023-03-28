import logging
import os
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from dvc_studio_client.post_live_metrics import get_studio_repo_url
from funcy import compact
from requests.adapters import HTTPAdapter

if TYPE_CHECKING:
    from requests import Response

logger = logging.getLogger(__name__)

STUDIO_URL = "https://studio.iterative.ai"
STUDIO_REPO_URL = "STUDIO_REPO_URL"
STUDIO_TOKEN = "STUDIO_TOKEN"  # noqa: S105


def get_studio_token_and_repo_url(
    default_token: Optional[str] = None,
    repo_url_finder: Optional[Callable[[], Optional[str]]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    token = os.getenv(STUDIO_TOKEN) or default_token
    url_finder = repo_url_finder or get_studio_repo_url
    repo_url = os.getenv(STUDIO_REPO_URL) or url_finder()
    return token, repo_url


def post(
    endpoint: str,
    token: str,
    data: Dict[str, Any],
    url: Optional[str] = STUDIO_URL,
    max_retries: int = 3,
    timeout: int = 5,
) -> "Response":
    endpoint = urljoin(url or STUDIO_URL, endpoint)
    session = requests.Session()
    session.mount(endpoint, HTTPAdapter(max_retries=max_retries))

    logger.trace("Sending %s to %s", data, endpoint)  # type: ignore[attr-defined]

    headers = {"Authorization": f"token {token}"}
    resp = session.post(endpoint, json=data, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp


def notify_refs(
    git_remote: str,
    default_token: Optional[str] = None,
    studio_url: Optional[str] = None,
    repo_url_finder: Optional[Callable[[], Optional[str]]] = None,
    **refs: List[str],
) -> None:
    # TODO: Should we use git_remote to associate with Studio project
    # instead of using `git ls-remote` on fallback?
    refs = compact(refs)
    if not refs:
        return

    assert git_remote
    token, repo_url = get_studio_token_and_repo_url(
        default_token=default_token,
        repo_url_finder=repo_url_finder,
    )
    if not token:
        logger.debug("Studio token not found.")
        return

    if not repo_url:
        logger.warning(
            "Could not detect repository url. "
            "Please set %s environment variable "
            "to your remote git repository url. ",
            STUDIO_REPO_URL,
        )
        return

    logger.debug(
        "notifying Studio%s about updated experiments",
        f" ({studio_url})" if studio_url else "",
    )
    data = {"repo_url": repo_url, "client": "dvc", "refs": refs}

    try:
        post("/webhook/dvc", token=token, data=data, url=studio_url)
    except requests.RequestException as e:
        logger.debug("", exc_info=True)

        msg = str(e)
        if (r := e.response) is not None:
            status = r.status_code
            # try to parse json response for more detailed error message
            try:
                d = r.json()
                logger.trace(  # type: ignore[attr-defined]
                    "received response: %s (status=%r)", d, status
                )
            except requests.JSONDecodeError:
                pass
            else:
                if detail := d.get("detail"):
                    msg = f"{detail} ({status=})"
        logger.warning("failed to notify Studio: %s", msg.lower())
