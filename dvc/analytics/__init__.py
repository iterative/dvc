import json
import logging
import platform
import requests
import subprocess
import sys
import tempfile

import distro

from dvc import __version__
from dvc.analytics import user_id
from dvc.config import Config, to_bool
from dvc.exceptions import NotDvcRepoError
from dvc.repo import Repo
from dvc.scm import SCM
from dvc.utils import env2bool, is_binary


logger = logging.getLogger(__name__)


def collect_and_send_report(arguments=None, exit_code=None):
    report = {
        "cmd_class": arguments.func.__name__,
        "cmd_return_code": exit_code,
        "dvc_version": __version__,
        "is_binary": is_binary(),
        "scm_class": scm_in_use(),
        "system_info": system_info(),
        "user_id": user_id.find_or_create(),
    }

    with tempfile.NamedTemporaryFile(delete=False, mode="w") as fobj:
        json.dump(report, fobj)
        subprocess.Popen(["python", "-m", "dvc.analytics", fobj.name])


def send(path):
    url = "https://analytics.dvc.org"

    with open(path) as fobj:
        report = json.load(fobj)

    requests.post(url, json=report, timeout=5)


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
