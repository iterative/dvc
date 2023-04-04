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
        r = post("/webhook/dvc", token, data, base_url=base_url)
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
