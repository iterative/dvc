import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast
from urllib.parse import urljoin

import requests
from funcy import compact, ignore
from requests.adapters import HTTPAdapter

if TYPE_CHECKING:
    from requests import Response

logger = logging.getLogger(__name__)

STUDIO_URL = "https://studio.iterative.ai"


def post(
    url: str,
    token: str,
    data: Dict[str, Any],
    base_url: Optional[str] = STUDIO_URL,
    max_retries: int = 3,
    timeout: int = 5,
) -> "Response":
    url = urljoin(base_url or STUDIO_URL, url)
    session = requests.Session()
    session.mount(url, HTTPAdapter(max_retries=max_retries))

    logger.trace("Sending %s to %s", data, url)  # type: ignore[attr-defined]

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
    **refs: List[str],
) -> Dict[str, Any]:
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
        logger.trace("", exc_info=True)  # type: ignore[attr-defined]

        msg = str(e)
        if e.response is None:
            logger.warning("failed to notify Studio: %s", msg.lower())
            return {}

        r = cast("Response", e.response)
        d = ignore(Exception, default={})(r.json)()
        status = r.status_code
        if detail := d.get("detail"):
            msg = f"{detail} ({status=})"
        logger.warning("failed to notify Studio: %s", msg.lower())
    else:
        d = r.json()

    if d:
        logger.trace(  # type: ignore[attr-defined]
            "received response: %s (status=%r)", d, r.status_code
        )
    return d


def get_studio_config(repo):
    """update studio config with current env and config values."""
    import os

    from dvc_studio_client.env import STUDIO_REPO_URL, STUDIO_TOKEN

    from dvc.config import to_bool
    from dvc.env import DVC_STUDIO_OFFLINE, DVC_STUDIO_TOKEN

    config = {}

    token = (
        os.getenv(DVC_STUDIO_TOKEN)
        or os.getenv(STUDIO_TOKEN)
        or repo.config.get("studio", {}).get("token")
    )
    if token:
        config[STUDIO_TOKEN] = token

    if STUDIO_REPO_URL in os.environ:
        config[STUDIO_REPO_URL] = os.getenv(STUDIO_REPO_URL)

    if DVC_STUDIO_OFFLINE in os.environ:
        config[DVC_STUDIO_OFFLINE] = to_bool(os.getenv(DVC_STUDIO_OFFLINE))
    elif "offline" in repo.config["studio"]:
        config[DVC_STUDIO_OFFLINE] = repo.config["studio"]["offline"]

    return config
