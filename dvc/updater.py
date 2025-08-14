import os
import time
from typing import TYPE_CHECKING, Optional

from packaging import version

from dvc import PKG, __version__
from dvc.env import DVC_UPDATER_ENDPOINT
from dvc.log import logger

if TYPE_CHECKING:
    from dvc.ui import RichText

logger = logger.getChild(__name__)


class Updater:
    URL = "https://updater.dvc.org"
    UPDATER_FILE = "updater"
    TIMEOUT = 24 * 60 * 60  # every day
    TIMEOUT_GET = 10

    def __init__(self, tmp_dir, friendly=False, hardlink_lock=False):
        from dvc.lock import make_lock

        self.updater_file = os.path.join(tmp_dir, self.UPDATER_FILE)
        self.lock = make_lock(
            self.updater_file + ".lock",
            tmp_dir=tmp_dir,
            friendly=friendly,
            hardlink_lock=hardlink_lock,
        )
        self.current = version.parse(__version__).base_version

    def _is_outdated_file(self):
        ctime = os.path.getmtime(self.updater_file)
        outdated = time.time() - ctime >= self.TIMEOUT
        if outdated:
            logger.debug("'%s' is outdated", self.updater_file)
        return outdated

    def _with_lock(self, func, action):
        from dvc.lock import LockError

        try:
            with self.lock:
                func()
        except LockError:
            logger.trace("", exc_info=True)
            logger.debug(
                "Failed to acquire '%s' before %s updates",
                self.lock.lockfile,
                action,
            )

    def check(self):
        from dvc.utils import env2bool

        if (
            os.getenv("CI")
            or env2bool("DVC_TEST")
            or PKG == "snap"
            or not self.is_enabled()
        ):
            return

        self._with_lock(self._check, "checking")

    def _check(self):
        if not os.path.exists(self.updater_file) or self._is_outdated_file():
            self.fetch()
            return

        with open(self.updater_file, encoding="utf-8") as fobj:
            import json

            try:
                info = json.load(fobj)
                latest = info["version"]
            except Exception as e:  # noqa: BLE001
                logger.trace("", exc_info=True)
                logger.debug("'%s' is not a valid json: %s", self.updater_file, e)
                self.fetch()
                return

        if version.parse(self.current) < version.parse(latest):
            self._notify(latest)

    def fetch(self, detach=True):
        from dvc.daemon import daemon

        if detach:
            daemon(["updater"])
            return

        self._with_lock(self._get_latest_version, "fetching")

    def _get_latest_version(self):
        import json

        import requests
        from requests.exceptions import RequestException

        url = os.environ.get(DVC_UPDATER_ENDPOINT, self.URL)
        logger.debug("Checking updates in %s", url)
        try:
            resp = requests.get(url, timeout=self.TIMEOUT_GET)
            info = resp.json()
        except RequestException as exc:
            logger.trace("", exc_info=True)
            logger.debug("Failed to retrieve latest version: %s", exc)
            return

        logger.trace("received payload: %s (status=%s)", info, resp.status_code)
        with open(self.updater_file, "w+", encoding="utf-8") as fobj:
            logger.trace("Saving latest version info to %s", self.updater_file)
            json.dump(info, fobj)

    def _notify(self, latest: str, pkg: Optional[str] = PKG) -> None:
        from dvc.ui import ui

        if not ui.isatty():
            return

        message = self._get_message(latest, pkg=pkg)
        return ui.error_write(message, styled=True)

    def _get_message(
        self,
        latest: str,
        current: Optional[str] = None,
        color: str = "yellow",
        pkg: Optional[str] = None,
    ) -> "RichText":
        from dvc.ui import ui

        current = current or self.current
        update_message = ui.rich_text.from_markup(
            f"You are using dvc version [bold]{current}[/]; "
            f"however, version [bold]{latest}[/] is available."
        )
        instruction = ui.rich_text.from_markup(self._get_update_instructions(pkg=pkg))
        return ui.rich_text.assemble(
            "\n", update_message, "\n", instruction, style=color
        )

    @staticmethod
    def _get_update_instructions(pkg: Optional[str] = None) -> str:
        if pkg in ("osxpkg", "exe", "binary"):
            return (
                "To upgrade, uninstall dvc and reinstall from [blue]https://dvc.org[/]."
            )

        instructions = {
            "pip": "pip install --upgrade dvc",
            "rpm": "yum update dvc",
            "brew": "brew upgrade dvc",
            "deb": "apt-get install --only-upgrade dvc",
            "conda": "conda update dvc",
            "choco": "choco upgrade dvc",
        }

        if pkg not in instructions:
            return (
                "Find the latest release at "
                "[blue]https://github.com/iterative/dvc/releases/latest[/]."
            )

        instruction = instructions[pkg]
        return f"To upgrade, run '{instruction}'."

    def is_enabled(self):
        from dvc.config import Config, to_bool

        enabled = to_bool(
            Config.from_cwd(validate=False).get("core", {}).get("check_update", "true")
        )
        logger.debug("Check for update is %sabled.", "en" if enabled else "dis")
        return enabled


def notify_updates():
    from contextlib import suppress

    from dvc.repo import NotDvcRepoError, Repo

    with suppress(NotDvcRepoError), Repo() as repo:
        hardlink_lock = repo.config["core"].get("hardlink_lock", False)
        updater = Updater(repo.tmp_dir, hardlink_lock=hardlink_lock)
        updater.check()
