import os
from collections import defaultdict
from copy import copy
from typing import TYPE_CHECKING, Dict, Optional, Set, Tuple, Union

from voluptuous import Required

from dvc.prompt import confirm

from .base import Dependency

if TYPE_CHECKING:
    from typing import ContextManager

    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.stage import Stage
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_data.hashfile.meta import Meta
    from dvc_data.hashfile.obj import HashFile
    from dvc_objects.db import ObjectDB


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

    def __init__(self, def_repo: Dict[str, str], stage: "Stage", *args, **kwargs):
        self.def_repo = def_repo
        self._objs: Dict[str, "HashFile"] = {}
        self._meta: Dict[str, "Meta"] = {}
        super().__init__(stage, *args, **kwargs)

    def _parse_path(self, fs, fs_path):  # noqa: ARG002
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

    def dumpd(self, **kwargs) -> Dict[str, Union[str, Dict[str, str]]]:
        return {self.PARAM_PATH: self.def_path, self.PARAM_REPO: self.def_repo}

    def download(self, to: "Output", jobs: Optional[int] = None):
        from dvc_data.hashfile.checkout import checkout

        for odb, objs in self.get_used_objs().items():
            self.repo.cloud.pull(objs, jobs=jobs, odb=odb)

        obj = self.get_obj()
        checkout(
            to.fs_path,
            to.fs,
            obj,
            self.repo.cache.local,
            ignore=None,
            state=self.repo.state,
            prompt=confirm,
        )

    def update(self, rev: Optional[str] = None):
        if rev:
            self.def_repo[self.PARAM_REV] = rev
        with self._make_repo(locked=False) as repo:
            self.def_repo[self.PARAM_REV_LOCK] = repo.get_rev()

    def changed_checksum(self) -> bool:
        # From current repo point of view what describes RepoDependency is its
        # origin project url and rev_lock, and it makes RepoDependency
        # immutable, hence its impossible for checksum to change.
        return False

    def get_used_objs(self, **kwargs) -> Dict[Optional["ObjectDB"], Set["HashInfo"]]:
        used, _, _ = self._get_used_and_obj(**kwargs)
        return used

    def _get_used_and_obj(
        self, obj_only: bool = False, **kwargs
    ) -> Tuple[Dict[Optional["ObjectDB"], Set["HashInfo"]], "Meta", "HashFile"]:
        from dvc.config import NoRemoteError
        from dvc.exceptions import NoOutputOrStageError, PathMissingError
        from dvc.utils import as_posix
        from dvc_data.hashfile.build import build
        from dvc_data.hashfile.tree import Tree, TreeError

        local_odb = self.repo.cache.local
        locked = kwargs.pop("locked", True)
        with self._make_repo(locked=locked, cache_dir=local_odb.path) as repo:
            used_obj_ids = defaultdict(set)
            rev = repo.get_rev()
            if locked and self.def_repo.get(self.PARAM_REV_LOCK) is None:
                self.def_repo[self.PARAM_REV_LOCK] = rev

            if not obj_only:
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
                        self._check_circular_import(odb, obj_ids)
                        used_obj_ids[odb].update(obj_ids)
                except (NoRemoteError, NoOutputOrStageError):
                    pass

            try:
                object_store, meta, obj = build(
                    local_odb,
                    as_posix(self.def_path),
                    repo.dvcfs,
                    local_odb.fs.PARAM_CHECKSUM,
                )
            except (FileNotFoundError, TreeError) as exc:
                raise PathMissingError(
                    self.def_path, self.def_repo[self.PARAM_URL]
                ) from exc
            object_store = copy(object_store)
            object_store.read_only = True

            self._objs[rev] = obj
            self._meta[rev] = meta

            used_obj_ids[object_store].add(obj.hash_info)
            if isinstance(obj, Tree):
                used_obj_ids[object_store].update(oid for _, _, oid in obj)
            return used_obj_ids, meta, obj

    def _check_circular_import(self, odb: "ObjectDB", obj_ids: Set["HashInfo"]) -> None:
        from dvc.exceptions import CircularImportError
        from dvc.fs.dvc import DVCFileSystem
        from dvc_data.hashfile.db.reference import ReferenceHashFileDB
        from dvc_data.hashfile.tree import Tree

        if not isinstance(odb, ReferenceHashFileDB):
            return

        def iter_objs():
            for hash_info in obj_ids:
                if hash_info.isdir:
                    tree = Tree.load(odb, hash_info)
                    yield from (odb.get(hi.value) for _, _, hi in tree)
                else:
                    assert hash_info.value
                    yield odb.get(hash_info.value)

        checked_urls = set()
        for obj in iter_objs():
            if not isinstance(obj.fs, DVCFileSystem):
                continue
            if obj.fs.repo_url in checked_urls or obj.fs.repo.root_dir in checked_urls:
                continue
            self_url = self.repo.url or self.repo.root_dir
            if (
                obj.fs.repo_url is not None
                and obj.fs.repo_url == self_url
                or obj.fs.repo.root_dir == self.repo.root_dir
            ):
                raise CircularImportError(self, obj.fs.repo_url, self_url)
            checked_urls.update([obj.fs.repo_url, obj.fs.repo.root_dir])

    def get_obj(self, filter_info=None, **kwargs):
        locked = kwargs.get("locked", True)
        rev = self._get_rev(locked=locked)
        if rev in self._objs:
            return self._objs[rev]
        _, _, obj = self._get_used_and_obj(
            obj_only=True, filter_info=filter_info, **kwargs
        )
        return obj

    def get_meta(self, filter_info=None, **kwargs):
        locked = kwargs.get("locked", True)
        rev = self._get_rev(locked=locked)
        if rev in self._meta:
            return self._meta[rev]
        _, meta, _ = self._get_used_and_obj(
            obj_only=True, filter_info=filter_info, **kwargs
        )
        return meta

    def _make_repo(self, locked: bool = True, **kwargs) -> "ContextManager[Repo]":
        from dvc.external_repo import external_repo

        d = self.def_repo
        rev = self._get_rev(locked=locked)
        return external_repo(d[self.PARAM_URL], rev=rev, **kwargs)

    def _get_rev(self, locked: bool = True):
        d = self.def_repo
        return (d.get(self.PARAM_REV_LOCK) if locked else None) or d.get(self.PARAM_REV)
