from copy import deepcopy
from typing import TYPE_CHECKING, Dict, Optional, Union

from voluptuous import Any, Required

from dvc.utils import as_posix

from .base import Dependency

if TYPE_CHECKING:
    from dvc.fs import DVCFileSystem
    from dvc.stage import Stage


class RepoDependency(Dependency):
    PARAM_REPO = "repo"
    PARAM_URL = "url"
    PARAM_REV = "rev"
    PARAM_REV_LOCK = "rev_lock"
    PARAM_CONFIG = "config"
    PARAM_REMOTE = "remote"

    REPO_SCHEMA = {
        PARAM_REPO: {
            Required(PARAM_URL): str,
            PARAM_REV: str,
            PARAM_REV_LOCK: str,
            PARAM_CONFIG: Any(str, dict),
            PARAM_REMOTE: Any(str, dict),
        }
    }

    def __init__(self, def_repo: Dict[str, Any], stage: "Stage", *args, **kwargs):
        self.def_repo = def_repo
        super().__init__(stage, *args, **kwargs)

        self.fs = self._make_fs()
        self.fs_path = as_posix(self.def_path)

    def _parse_path(self, fs, fs_path):  # noqa: ARG002
        return None

    @property
    def is_in_repo(self):
        return False

    def __str__(self):
        return f"{self.def_path} ({self.def_repo[self.PARAM_URL]})"

    def workspace_status(self):
        current = self._make_fs(locked=True).repo.get_rev()
        updated = self._make_fs(locked=False).repo.get_rev()

        if current != updated:
            return {str(self): "update available"}

        return {}

    def status(self):
        return self.workspace_status()

    def save(self):
        rev = self.fs.repo.get_rev()
        if self.def_repo.get(self.PARAM_REV_LOCK) is None:
            self.def_repo[self.PARAM_REV_LOCK] = rev

    def dumpd(self, **kwargs) -> Dict[str, Union[str, Dict[str, str]]]:
        repo = {self.PARAM_URL: self.def_repo[self.PARAM_URL]}

        rev = self.def_repo.get(self.PARAM_REV)
        if rev:
            repo[self.PARAM_REV] = self.def_repo[self.PARAM_REV]

        rev_lock = self.def_repo.get(self.PARAM_REV_LOCK)
        if rev_lock:
            repo[self.PARAM_REV_LOCK] = rev_lock

        config = self.def_repo.get(self.PARAM_CONFIG)
        if config:
            repo[self.PARAM_CONFIG] = config

        remote = self.def_repo.get(self.PARAM_REMOTE)
        if remote:
            repo[self.PARAM_REMOTE] = remote

        return {
            self.PARAM_PATH: self.def_path,
            self.PARAM_REPO: repo,
        }

    def update(self, rev: Optional[str] = None):
        if rev:
            self.def_repo[self.PARAM_REV] = rev
        self.fs = self._make_fs(rev=rev, locked=False)
        self.def_repo[self.PARAM_REV_LOCK] = self.fs.repo.get_rev()

    def changed_checksum(self) -> bool:
        # From current repo point of view what describes RepoDependency is its
        # origin project url and rev_lock, and it makes RepoDependency
        # immutable, hence its impossible for checksum to change.
        return False

    def _make_fs(
        self, rev: Optional[str] = None, locked: bool = True
    ) -> "DVCFileSystem":
        from dvc.config import Config
        from dvc.fs import DVCFileSystem

        rem = self.def_repo.get("remote")
        if isinstance(rem, dict):
            remote = None
            remote_config = rem
        else:
            remote = rem
            remote_config = None

        conf = self.def_repo.get("config", {})
        if isinstance(conf, dict):
            config = deepcopy(conf)
        else:
            config = Config.load_file(conf)

        config["cache"] = self.repo.config["cache"]
        config["cache"]["dir"] = self.repo.cache.local_cache_dir

        return DVCFileSystem(
            url=self.def_repo[self.PARAM_URL],
            rev=rev or self._get_rev(locked=locked),
            subrepos=True,
            config=config,
            remote=remote,
            remote_config=remote_config,
        )

    def _get_rev(self, locked: bool = True):
        d = self.def_repo
        return (d.get(self.PARAM_REV_LOCK) if locked else None) or d.get(self.PARAM_REV)
