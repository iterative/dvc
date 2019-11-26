import json
import logging
import platform
import requests
import sys
import tempfile
import uuid

import distro

from dvc import __version__
from dvc.config import Config, to_bool
from dvc.daemon import daemon
from dvc.exceptions import NotDvcRepoError
from dvc.lock import Lock, LockError
from dvc.repo import Repo
from dvc.scm import SCM
from dvc.utils import env2bool, is_binary, makedirs

logger = logging.getLogger(__name__)


def collect_and_send_report(arguments=None, exit_code=None):
    """
    Query the system to fill a report and send it on a detached process.

    A temporary file is used as a mean of communication between the
    current and detached process.
    """
    report = {
        "cmd_class": arguments.func.__name__,
        "cmd_return_code": exit_code,
        "dvc_version": __version__,
        "is_binary": is_binary(),
        "scm_class": scm_in_use(),
        "system_info": system_info(),
        "user_id": find_or_create_user_id(),
    }

    with tempfile.NamedTemporaryFile(delete=False, mode="w") as fobj:
        json.dump(report, fobj)
        daemon(["analytics", fobj.name])


def is_enabled():
    if env2bool("DVC_TEST"):
        return False

    enabled = to_bool(
        Config(validate=False)
        .config.get(Config.SECTION_CORE, {})
        .get(Config.SECTION_CORE_ANALYTICS, "true")
    )

    logger.debug("Analytics is {}enabled.".format("" if enabled else "dis"))

    return enabled


def send(path):
    url = "https://analytics.dvc.org"

    with open(path) as fobj:
        report = json.load(fobj)

    requests.post(url, json=report, timeout=5)


def scm_in_use():
    try:
        scm = SCM(root_dir=Repo.find_root())
        return type(scm).__name__
    except NotDvcRepoError:
        pass


def system_info():
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

    return {"os": system.lower()}


def find_or_create_user_id():
    """
    The user's ID is stored on a file under the global config directory.

    The file should contain a JSON with a "user_id" key:

        {"user_id": "16fd2706-8baf-433b-82eb-8c7fada847da"}

    IDs are generated randomly with UUID.
    """
    config_dir = Config.get_global_config_dir()
    fname = config_dir / "user_id"
    lockfile = fname.with_suffix(".lock")

    try:
        with Lock(lockfile):
            try:
                user_id = json.load(fname.read_text())["user_id"]
            except (FileNotFoundError, json.JSONDecodeError, AttributeError):
                user_id = str(uuid.uuid4())
                makedirs(fname.parent, exist_ok=True)
                fname.write_text(json.dumps({"user_id": user_id}))

            return user_id

    except LockError:
        logger.debug("Failed to acquire {lock}".format(lockfile))
