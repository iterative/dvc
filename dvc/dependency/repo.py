import errno
import os
from collections import defaultdict
from copy import copy, deepcopy
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

import voluptuous as vol

from dvc.exceptions import DvcException
from dvc.prompt import confirm
from dvc.utils import as_posix

from .base import Dependency

if TYPE_CHECKING:
    from dvc.fs import DVCFileSystem
    from dvc.output import Output
    from dvc.stage import Stage
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_data.hashfile.obj import HashFile
    from dvc_objects.db import ObjectDB


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
        from dvc_data.hashfile.checkout import checkout

        try:
            used, obj = self._get_used_and_obj()
            for odb, objs in used.items():
                self.repo.cloud.pull(objs, jobs=jobs, odb=odb)

            checkout(
                to.fs_path,
                to.fs,
                obj,
                self.repo.cache.local,
                ignore=None,
                state=self.repo.state,
                prompt=confirm,
            )
        except DvcException:
            super().download(to=to, jobs=jobs)

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

    def _get_used_and_obj(
        self, **kwargs
    ) -> tuple[dict[Optional["ObjectDB"], set["HashInfo"]], "HashFile"]:
        from dvc.config import NoRemoteError
        from dvc.exceptions import NoOutputOrStageError
        from dvc.utils import as_posix
        from dvc_data.hashfile.build import build
        from dvc_data.hashfile.tree import Tree, TreeError

        local_odb = self.repo.cache.local
        locked = kwargs.pop("locked", True)
        repo = self._make_fs(locked=locked).repo
        used_obj_ids = defaultdict(set)
        rev = repo.get_rev()
        if locked and self.def_repo.get(self.PARAM_REV_LOCK) is None:
            self.def_repo[self.PARAM_REV_LOCK] = rev

        try:
            for odb, obj_ids in repo.used_objs(
                [os.path.join(repo.root_dir, self.def_path)],
                force=True,
                jobs=kwargs.get("jobs"),
                recursive=True,
            ).items():
                if odb is None:
                    odb = repo.cloud.get_remote_odb()
                    odb.read_only = True
                used_obj_ids[odb].update(obj_ids)
        except (NoRemoteError, NoOutputOrStageError):
            pass

        try:
            object_store, _, obj = build(
                local_odb,
                as_posix(self.def_path),
                repo.dvcfs,
                local_odb.fs.PARAM_CHECKSUM,
            )
        except (FileNotFoundError, TreeError) as exc:
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT) + f" in {self.def_repo[self.PARAM_URL]}",
                self.def_path,
            ) from exc
        object_store = copy(object_store)
        object_store.read_only = True

        used_obj_ids[object_store].add(obj.hash_info)
        if isinstance(obj, Tree):
            used_obj_ids[object_store].update(oid for _, _, oid in obj)
        return used_obj_ids, obj

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
