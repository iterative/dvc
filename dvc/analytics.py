import json
import os
from typing import TYPE_CHECKING, Optional

from dvc.log import logger

from .env import DVC_ANALYTICS_ENDPOINT, DVC_NO_ANALYTICS

if TYPE_CHECKING:
    from dvc.scm import Base

logger = logger.getChild(__name__)


def collect_and_send_report(args=None, return_code=None):
    """
    Collect information from the runtime/environment and the command
    being executed into a report and send it over the network.

    To prevent analytics from blocking the execution of the main thread,
    sending the report is done in a separate process.

    The inter-process communication happens through a file containing the
    report as a JSON, where the _collector_ generates it and the _sender_
    removes it after sending it.
    """
    import tempfile

    from dvc.daemon import daemon

    report = {}

    # Include command execution information on the report only when available.
    if args and hasattr(args, "func"):
        report.update({"cmd_class": args.func.__name__})

    if return_code is not None:
        report.update({"cmd_return_code": return_code})

    with tempfile.NamedTemporaryFile(delete=False, mode="w") as fobj:
        json.dump(report, fobj)

    logger.trace("Saving analytics report to %s", fobj.name)
    daemon(["analytics", fobj.name])


def is_enabled():
    from dvc.config import Config, to_bool
    from dvc.utils import env2bool

    if env2bool("DVC_TEST"):
        return False

    enabled = not os.getenv(DVC_NO_ANALYTICS)
    if enabled:
        enabled = to_bool(
            Config.from_cwd(validate=False).get("core", {}).get("analytics", "true")
        )

    logger.debug("Analytics is %sabled.", "en" if enabled else "dis")

    return enabled


def send(path):
    """
    Side effect: Removes the report after sending it.

    The report is generated and stored in a temporary file, see:
    `collect_and_send_report`. Sending happens on another process,
    thus, the need of removing such file afterwards.
    """
    import requests
    from requests.exceptions import RequestException

    url = os.environ.get(DVC_ANALYTICS_ENDPOINT, "https://analytics.dvc.org")
    headers = {"content-type": "application/json"}

    with open(path, encoding="utf-8") as fobj:
        report = json.load(fobj)

    report.update(_runtime_info())

    logger.debug("uploading report to %s", url)
    logger.trace("Sending %s to %s", report, url)

    try:
        requests.post(url, json=report, headers=headers, timeout=5)
    except RequestException as e:
        logger.trace("", exc_info=True)
        logger.debug("failed to send analytics report %s", str(e))

    logger.trace("removing report %s", path)
    os.remove(path)


def _git_remote_url(scm: Optional["Base"]) -> Optional[str]:
    from dvc.scm import Git

    if not isinstance(scm, Git):
        return None

    from dulwich.porcelain import get_remote_repo

    dulwich_repo = scm.dulwich.repo
    try:
        _remote, url = get_remote_repo(dulwich_repo)
    except IndexError:
        # IndexError happens when the head is detached
        _remote, url = get_remote_repo(dulwich_repo, b"origin")
    # Dulwich returns (None, "origin") if no remote set
    if (_remote, url) == (None, "origin"):
        return None
    return url


def _scm_in_use(scm: Optional["Base"]) -> Optional[str]:
    return type(scm).__name__ if scm else None


def _parse_git_remote_path(remote_url: str) -> str:
    from urllib.parse import urlparse

    from scmrepo.urls import is_scp_style_url

    parsed = urlparse(remote_url)
    # Windows Path also gets parsed with a drive letter as scheme
    # https://github.com/python/cpython/issues/86381
    if parsed.scheme and parsed.scheme in ("http", "https", "git", "ssh"):
        return parsed.path.strip("/")

    if is_scp_style_url(remote_url):
        # handle scp-style URL
        parts = remote_url.split(":", 1)
        if len(parts) == 2:
            _, path = parts
            return path.rstrip("/")
    return remote_url


def _git_remote_path_hash(scm: Optional["Base"]) -> Optional[str]:
    """Return a hash of the git remote path."""
    import hashlib

    try:
        if remote_url := _git_remote_url(scm):
            path = _parse_git_remote_path(remote_url)
            h = hashlib.md5(path.encode("utf-8"), usedforsecurity=False)  # for FIPS
            return h.hexdigest()
    except Exception:  # noqa: BLE001
        logger.debug("Failed to get git remote path", exc_info=True)
    return None


def _runtime_info():
    """
    Gather information from the environment where DVC runs to fill a report.
    """
    from iterative_telemetry import _generate_ci_id, find_or_create_user_id

    from dvc import __version__
    from dvc.info import _get_remotes
    from dvc.repo import Repo
    from dvc.utils import is_binary

    ci_id = _generate_ci_id()
    if ci_id:
        group_id, user_id = ci_id
    else:
        group_id, user_id = None, find_or_create_user_id()

    scm = None
    remotes = None
    try:
        repo = Repo()
        scm = repo.scm
        remotes = _get_remotes(repo.config)
    except Exception as exc:  # noqa: BLE001
        logger.debug("failed to open repo: %s", exc)

    return {
        "dvc_version": __version__,
        "is_binary": is_binary(),
        "scm_class": _scm_in_use(scm),
        "system_info": _system_info(),
        "user_id": user_id,
        "group_id": group_id,
        "remotes": remotes,
        "git_remote_hash": _git_remote_path_hash(scm),
    }


def _system_info():
    import platform
    import sys

    import distro

    system = platform.system()

    if system == "Windows":
        version = sys.getwindowsversion()  # type: ignore[attr-defined]

        return {
            "os": "windows",
            "windows_version_build": version.build,
            "windows_version_major": version.major,
            "windows_version_minor": version.minor,
            "windows_version_service_pack": version.service_pack,
        }

    if system == "Darwin":
        return {"os": "mac", "mac_version": platform.mac_ver()[0]}

    if system == "Linux":
        return {
            "os": "linux",
            "linux_distro": distro.id(),
            "linux_distro_like": distro.like(),
            "linux_distro_version": distro.version(),
        }

    # We don't collect data for any other system.
    raise NotImplementedError
