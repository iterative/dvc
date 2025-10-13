from copy import deepcopy
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

import voluptuous as vol

from dvc.utils import as_posix

from .base import Dependency

if TYPE_CHECKING:
    from dvc.fs import DVCFileSystem
    from dvc.output import Output
    from dvc.stage import Stage
    from dvc_data.hashfile.hash_info import HashInfo


class RepoDependency(Dependency):
    PARAM_REPO = "repo"
    PARAM_URL = "url"
    PARAM_REV = "rev"
    PARAM_REV_LOCK = "rev_lock"
    PARAM_CONFIG = "config"
    PARAM_REMOTE = "remote"

    REPO_SCHEMA: ClassVar[dict] = {
        PARAM_REPO: {
            vol.Required(PARAM_URL): str,
            PARAM_REV: str,
            PARAM_REV_LOCK: str,
            PARAM_CONFIG: vol.Any(str, dict),
            PARAM_REMOTE: vol.Any(str, dict),
        }
    }

    def __init__(self, def_repo: dict[str, Any], stage: "Stage", *args, **kwargs):
        self.def_repo = def_repo
        super().__init__(stage, *args, **kwargs)

        self.fs = self._make_fs()
        self.fs_path = as_posix(self.fs.normpath(self.def_path))

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

    @classmethod
    def _dump_def_repo(cls, def_repo) -> dict[str, str]:
        repo = {cls.PARAM_URL: def_repo[cls.PARAM_URL]}

        rev = def_repo.get(cls.PARAM_REV)
        if rev:
            repo[cls.PARAM_REV] = def_repo[cls.PARAM_REV]

        rev_lock = def_repo.get(cls.PARAM_REV_LOCK)
        if rev_lock:
            repo[cls.PARAM_REV_LOCK] = rev_lock

        config = def_repo.get(cls.PARAM_CONFIG)
        if config:
            repo[cls.PARAM_CONFIG] = config

        remote = def_repo.get(cls.PARAM_REMOTE)
        if remote:
            repo[cls.PARAM_REMOTE] = remote
        return repo

    def dumpd(self, **kwargs) -> dict[str, Union[str, dict[str, str]]]:
        return {
            self.PARAM_PATH: self.def_path,
            self.PARAM_REPO: self._dump_def_repo(self.def_repo),
        }

    def download(self, to: "Output", jobs: Optional[int] = None):
        from dvc.fs import LocalFileSystem

        files = super().download(to=to, jobs=jobs)
        if not isinstance(to.fs, LocalFileSystem):
            return

        hashes: list[tuple[str, HashInfo, dict[str, Any]]] = []
        for src_path, dest_path, maybe_info in files:
            try:
                info = maybe_info or self.fs.info(src_path)
                hash_info = info["dvc_info"]["entry"].hash_info
                dest_info = to.fs.info(dest_path)
            except (KeyError, AttributeError):
                # If no hash info found, just keep going and output will be hashed later
                continue
            if hash_info:
                hashes.append((dest_path, hash_info, dest_info))
        cache = to.cache if to.use_cache else to.local_cache
        cache.state.save_many(hashes, to.fs)

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

        # Setup config to the new DVCFileSystem to use the remote repo, but rely on the
        # local cache instead of the remote's cache. This avoids re-streaming of data,
        # but messes up the call to `_get_remote_config()` downstream, which will need
        # to ignore cache parameters.
        assert self.repo
        config["cache"] = self.repo.config["cache"]
        config["cache"]["dir"] = self.repo.cache.local_cache_dir

        return DVCFileSystem(
            repo=self.def_repo[self.PARAM_URL],
            rev=rev or self._get_rev(locked=locked),
            subrepos=True,
            config=config,
            remote=remote,
            remote_config=remote_config,
        )

    def _get_rev(self, locked: bool = True):
        d = self.def_repo
        return (d.get(self.PARAM_REV_LOCK) if locked else None) or d.get(self.PARAM_REV)
