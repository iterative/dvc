import json
import logging
import os

from .env import DVC_NO_ANALYTICS

logger = logging.getLogger(__name__)


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
    daemon(["analytics", fobj.name])


def is_enabled():
    from dvc.config import Config, to_bool
    from dvc.utils import env2bool

    if env2bool("DVC_TEST"):
        return False

    enabled = not os.getenv(DVC_NO_ANALYTICS)
    if enabled:
        enabled = to_bool(
            Config(validate=False).get("core", {}).get("analytics", "true")
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

    url = "https://analytics.dvc.org"
    headers = {"content-type": "application/json"}

    with open(path, encoding="utf-8") as fobj:
        report = json.load(fobj)

    report.update(_runtime_info())

    try:
        requests.post(url, json=report, headers=headers, timeout=5)
    except requests.exceptions.RequestException:
        logger.debug("failed to send analytics report", exc_info=True)

    os.remove(path)


def _scm_in_use():
    from scmrepo.noscm import NoSCM

    from dvc.exceptions import NotDvcRepoError
    from dvc.repo import Repo

    from .scm import SCM, SCMError

    try:
        scm = SCM(root_dir=Repo.find_root())
        return type(scm).__name__
    except SCMError:
        return NoSCM.__name__
    except NotDvcRepoError:
        pass


def _runtime_info():
    """
    Gather information from the environment where DVC runs to fill a report.
    """
    from iterative_telemetry import find_or_create_user_id

    from dvc import __version__
    from dvc.utils import is_binary

    return {
        "dvc_version": __version__,
        "is_binary": is_binary(),
        "scm_class": _scm_in_use(),
        "system_info": _system_info(),
        "user_id": find_or_create_user_id(),
    }


def _system_info():
    import platform
    import sys

    import distro

    system = platform.system()

    if system == "Windows":
        version = sys.getwindowsversion()

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
