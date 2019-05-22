from __future__ import unicode_literals

import sys
import os
import time
import logging
import requests
import colorama
from pkg_resources import parse_version

from dvc import __version__
from dvc.lock import Lock, LockError
from dvc.utils import is_binary, boxify


logger = logging.getLogger(__name__)


class Updater(object):  # pragma: no cover
    URL = "https://updater.dvc.org"
    UPDATER_FILE = "updater"
    TIMEOUT = 24 * 60 * 60  # every day
    TIMEOUT_GET = 10

    def __init__(self, dvc_dir):
        self.dvc_dir = dvc_dir
        self.updater_file = os.path.join(dvc_dir, self.UPDATER_FILE)
        self.lock = Lock(dvc_dir, self.updater_file + ".lock")
        self.current = parse_version(__version__).base_version

    def _is_outdated_file(self):
        ctime = os.path.getmtime(self.updater_file)
        outdated = time.time() - ctime >= self.TIMEOUT
        if outdated:
            logger.debug("'{}' is outdated(".format(self.updater_file))
        return outdated

    def _with_lock(self, func, action):
        try:
            with self.lock:
                func()
        except LockError:
            msg = "Failed to acquire '{}' before {} updates"
            logger.debug(msg.format(self.lock.lock_file, action))

    def check(self):
        if os.getenv("CI") or os.getenv("DVC_TEST"):
            return

        self._with_lock(self._check, "checking")

    def _check(self):
        if not os.path.exists(self.updater_file) or self._is_outdated_file():
            self.fetch()
            return

        with open(self.updater_file, "r") as fobj:
            import json

            try:
                info = json.load(fobj)
                self.latest = info["version"]
            except Exception as exc:
                msg = "'{}' is not a valid json: {}"
                logger.debug(msg.format(self.updater_file, exc))
                self.fetch()
                return

        if self._is_outdated():
            self._notify()

    def fetch(self, detach=True):
        from dvc.daemon import daemon

        if detach:
            daemon(["updater"])
            return

        self._with_lock(self._get_latest_version, "fetching")

    def _get_latest_version(self):
        import json

        try:
            r = requests.get(self.URL, timeout=self.TIMEOUT_GET)
            info = r.json()
        except requests.exceptions.RequestException as exc:
            msg = "Failed to retrieve latest version: {}"
            logger.debug(msg.format(exc))
            return

        with open(self.updater_file, "w+") as fobj:
            json.dump(info, fobj)

    def _is_outdated(self):
        return parse_version(self.current) < parse_version(self.latest)

    def _notify(self):
        if not sys.stdout.isatty():
            return

        message = (
            "Update available {red}{current}{reset} -> {green}{latest}{reset}"
            + "\n"
            + self._get_update_instructions()
        ).format(
            red=colorama.Fore.RED,
            reset=colorama.Fore.RESET,
            green=colorama.Fore.GREEN,
            yellow=colorama.Fore.YELLOW,
            blue=colorama.Fore.BLUE,
            current=self.current,
            latest=self.latest,
        )

        logger.info(boxify(message, border_color="yellow"))

    def _get_update_instructions(self):
        instructions = {
            "pip": "Run {yellow}pip{reset} install dvc {blue}--upgrade{reset}",
            "yum": "Run {yellow}yum{reset} update dvc",
            "yay": "Run {yellow}yay{reset} {blue}-S{reset} dvc",
            "formula": "Run {yellow}brew{reset} upgrade dvc",
            "cask": "Run {yellow}brew cask{reset} upgrade dvc",
            "apt": (
                "Run {yellow}apt-get{reset} install"
                " {blue}--only-upgrade{reset} dvc"
            ),
            "binary": (
                "To upgrade follow this steps:\n"
                "1. Uninstall dvc binary\n"
                "2. Go to {blue}https://dvc.org{reset}\n"
                "3. Download and install new binary"
            ),
            None: (
                "Find the latest release at\n{blue}"
                "https://github.com/iterative/dvc/releases/latest"
                "{reset}"
            ),
        }

        package_manager = self._get_package_manager()

        return instructions[package_manager]

    def _get_linux(self):
        import distro

        if not is_binary():
            return "pip"

        package_managers = {
            "rhel": "yum",
            "centos": "yum",
            "fedora": "yum",
            "amazon": "yum",
            "opensuse": "yum",
            "ubuntu": "apt",
            "debian": "apt",
        }

        return package_managers.get(distro.id())

    def _get_darwin(self):
        if not is_binary():
            if __file__.startswith("/usr/local/Cellar"):
                return "formula"
            else:
                return "pip"

        # NOTE: both pkg and cask put dvc binary into /usr/local/bin,
        # so in order to know which method of installation was used,
        # we need to actually call `brew cask`
        ret = os.system("brew cask ls dvc")
        if ret == 0:
            return "cask"

        return None

    def _get_windows(self):
        return None if is_binary() else "pip"

    def _get_package_manager(self):
        import platform
        from dvc.exceptions import DvcException

        m = {
            "Windows": self._get_windows,
            "Darwin": self._get_darwin,
            "Linux": self._get_linux,
        }

        system = platform.system()
        func = m.get(system)
        if func is None:
            raise DvcException("not supported system '{}'".format(system))

        return func()
