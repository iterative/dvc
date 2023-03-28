import logging
import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from funcy import compact
from requests.adapters import HTTPAdapter

if TYPE_CHECKING:
    from requests import Response

logger = logging.getLogger(__name__)

STUDIO_URL = "https://studio.iterative.ai"
STUDIO_TOKEN = "STUDIO_TOKEN"  # noqa: S105


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
    repo_url: str,
    *,
    default_token: Optional[str] = None,
    studio_url: Optional[str] = None,
    **refs: List[str],
) -> None:
    extra_keys = refs.keys() - {"pushed", "removed"}
    assert not extra_keys, f"got extra args: {extra_keys}"

    refs = compact(refs)
    if not refs:
        return

    token = os.getenv(STUDIO_TOKEN) or default_token
    if not token:
        logger.debug("Studio token not found.")
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
