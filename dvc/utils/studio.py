from typing import TYPE_CHECKING, Any, Dict, List, Optional
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

if TYPE_CHECKING:
    from requests import Response

logger = logger.getChild(__name__)

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

    logger.trace("Sending %s to %s", data, url)

    headers = {"Authorization": f"token {token}"} if token else None
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


def start_device_login(
    *,
    data,
    base_url=STUDIO_URL,
):
    logger.debug(
        "Starting device login for Studio%s",
        f" ({base_url})" if base_url else "",
    )

    r = post("api/device-login", "", data=data, base_url=base_url)
    if r.status_code == 400:
        logger.error(
            "Failed to start authentication with Studio: %s", r.json().get("detail")
        )
        return

    r.raise_for_status()
    d = r.json()

    logger.trace(  # type: ignore[attr-defined]
        "received response: %s (status=%r)", d, r.status_code
    )
    return d


def check_token_authorization(*, uri, device_code):
    import time

    logger.debug("Polling to find if the user code is authorized")

    data = {"code": device_code}
    session = requests.Session()
    session.mount(uri, HTTPAdapter(max_retries=3))

    logger.debug("Checking with %s to %s", device_code, uri)

    counter = 1
    while True:
        logger.debug("Polling attempt #%s", counter)
        r = session.post(uri, json=data, timeout=5, allow_redirects=False)
        counter += 1
        if r.status_code == 400:
            d = ignore(Exception, default={})(r.json)()
            detail = d.get("detail")
            if detail == "authorization_pending":
                # Wait 5 seconds before retrying.
                time.sleep(5)
                continue
            if detail == "authorization_expired":
                return

        r.raise_for_status()

        return r.json()["access_token"]


def config_to_env(config: Dict[str, Any]) -> Dict[str, Any]:
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


def env_to_config(env: Dict[str, Any]) -> Dict[str, Any]:
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
