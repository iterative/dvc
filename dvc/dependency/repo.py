import os
from collections import defaultdict
from copy import copy
from typing import TYPE_CHECKING, Dict, Optional, Set, Tuple

from voluptuous import Required

from .base import Dependency

if TYPE_CHECKING:
    from dvc.hash_info import HashInfo
    from dvc.objects.db import ObjectDB
    from dvc.objects.file import HashFile


class RepoDependency(Dependency):
    PARAM_REPO = "repo"
    PARAM_URL = "url"
    PARAM_REV = "rev"
    PARAM_REV_LOCK = "rev_lock"

    REPO_SCHEMA = {
        PARAM_REPO: {
            Required(PARAM_URL): str,
            PARAM_REV: str,
            PARAM_REV_LOCK: str,
        }
    }

    def __init__(self, def_repo, stage, *args, **kwargs):
        self.def_repo = def_repo
        self._staged_objs: Dict[str, "HashFile"] = {}
        super().__init__(stage, *args, **kwargs)

    def _parse_path(self, fs, fs_path):
        return None

    @property
    def is_in_repo(self):
        return False

    def __str__(self):
        return f"{self.def_path} ({self.def_repo[self.PARAM_URL]})"

    def workspace_status(self):
        current = self.get_obj(locked=True).hash_info
        updated = self.get_obj(locked=False).hash_info

        if current != updated:
            return {str(self): "update available"}

        return {}

    def status(self):
        return self.workspace_status()

    def save(self):
        pass

    def dumpd(self):
        return {self.PARAM_PATH: self.def_path, self.PARAM_REPO: self.def_repo}

    def download(self, to, jobs=None):
        from dvc.data.checkout import checkout

        for odb, objs in self.get_used_objs().items():
            self.repo.cloud.pull(objs, jobs=jobs, odb=odb)

        obj = self.get_obj()
        checkout(
            to.fs_path,
            to.fs,
            obj,
            self.repo.odb.local,
            dvcignore=None,
            state=self.repo.state,
        )

    def update(self, rev=None):
        if rev:
            self.def_repo[self.PARAM_REV] = rev
        with self._make_repo(locked=False) as repo:
            self.def_repo[self.PARAM_REV_LOCK] = repo.get_rev()

    def changed_checksum(self):
        # From current repo point of view what describes RepoDependency is its
        # origin project url and rev_lock, and it makes RepoDependency
        # immutable, hence its impossible for checksum to change.
        return False

    def get_used_objs(
        self, **kwargs
    ) -> Dict[Optional["ObjectDB"], Set["HashInfo"]]:
        used, _ = self._get_used_and_obj(**kwargs)
        return used

    def _get_used_and_obj(
        self, obj_only=False, **kwargs
    ) -> Tuple[Dict[Optional["ObjectDB"], Set["HashInfo"]], "HashFile"]:
        from dvc.config import NoRemoteError
        from dvc.data.stage import stage
        from dvc.data.tree import Tree, TreeError
        from dvc.exceptions import NoOutputOrStageError, PathMissingError

        local_odb = self.repo.odb.local
        locked = kwargs.pop("locked", True)
        with self._make_repo(
            locked=locked, cache_dir=local_odb.cache_dir
        ) as repo:
            used_obj_ids = defaultdict(set)
            rev = repo.get_rev()
            if locked and self.def_repo.get(self.PARAM_REV_LOCK) is None:
                self.def_repo[self.PARAM_REV_LOCK] = rev

            path = os.path.abspath(os.path.join(repo.root_dir, self.def_path))
            if not obj_only:
                try:
                    for odb, obj_ids in repo.used_objs(
                        [path],
                        force=True,
                        jobs=kwargs.get("jobs"),
                        recursive=True,
                    ).items():
                        if odb is None:
                            odb = repo.cloud.get_remote_odb()
                            odb.read_only = True
                        self._check_circular_import(odb, obj_ids)
                        used_obj_ids[odb].update(obj_ids)
                except (NoRemoteError, NoOutputOrStageError):
                    pass

            try:
                staging, _, staged_obj = stage(
                    local_odb,
                    path,
                    repo.repo_fs,
                    local_odb.fs.PARAM_CHECKSUM,
                )
            except (FileNotFoundError, TreeError) as exc:
                raise PathMissingError(
                    self.def_path, self.def_repo[self.PARAM_URL]
                ) from exc
            staging = copy(staging)
            staging.read_only = True

            self._staged_objs[rev] = staged_obj
            used_obj_ids[staging].add(staged_obj.hash_info)
            if isinstance(staged_obj, Tree):
                used_obj_ids[staging].update(oid for _, _, oid in staged_obj)
            return used_obj_ids, staged_obj

    def _check_circular_import(self, odb, obj_ids):
        from dvc.data.db.reference import ReferenceObjectDB
        from dvc.data.tree import Tree
        from dvc.exceptions import CircularImportError
        from dvc.fs.repo import RepoFileSystem

        if not isinstance(odb, ReferenceObjectDB):
            return

        def iter_objs():
            for hash_info in obj_ids:
                if hash_info.isdir:
                    tree = Tree.load(odb, hash_info)
                    yield from (odb.get(oid) for _, _, oid in tree)
                else:
                    yield odb.get(hash_info)

        checked_urls = set()
        for obj in iter_objs():
            if not isinstance(obj.fs, RepoFileSystem):
                continue
            if (
                obj.fs.repo_url in checked_urls
                or obj.fs.root_dir in checked_urls
            ):
                continue
            self_url = self.repo.url or self.repo.root_dir
            if (
                obj.fs.repo_url is not None
                and obj.fs.repo_url == self_url
                or obj.fs.root_dir == self.repo.root_dir
            ):
                raise CircularImportError(self, obj.fs.repo_url, self_url)
            checked_urls.update([obj.fs.repo_url, obj.fs.root_dir])

    def get_obj(self, filter_info=None, **kwargs):
        locked = kwargs.get("locked", True)
        rev = self._get_rev(locked=locked)
        if rev in self._staged_objs:
            return self._staged_objs[rev]
        _, obj = self._get_used_and_obj(
            obj_only=True, filter_info=filter_info, **kwargs
        )
        return obj

    def _make_repo(self, locked=True, **kwargs):
        from dvc.external_repo import external_repo

        d = self.def_repo
        rev = self._get_rev(locked=locked)
        return external_repo(d[self.PARAM_URL], rev=rev, **kwargs)

    def _get_rev(self, locked=True):
        d = self.def_repo
        return (d.get(self.PARAM_REV_LOCK) if locked else None) or d.get(
            self.PARAM_REV
        )
