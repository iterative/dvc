import os
from typing import TYPE_CHECKING, Dict, List

from voluptuous import Required

from dvc.objects import UsedObjectsPair
from dvc.path_info import PathInfo

from .base import Dependency

if TYPE_CHECKING:
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

    def _parse_path(self, fs, path_info):
        return None

    @property
    def is_in_repo(self):
        return False

    def __str__(self):
        return "{} ({})".format(self.def_path, self.def_repo[self.PARAM_URL])

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
        from dvc.checkout import checkout
        from dvc.objects import load, save

        odb = self.repo.odb.local

        obj = self.get_obj()
        save(odb, obj, jobs=jobs)

        obj = load(odb, obj.hash_info)
        checkout(
            to.path_info,
            to.fs,
            obj,
            odb,
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

    def get_used_objs(self, **kwargs) -> List[UsedObjectsPair]:
        from dvc.config import NoRemoteError
        from dvc.exceptions import NoOutputOrStageError
        from dvc.objects.stage import stage

        odb = self.repo.odb.local
        locked = kwargs.pop("locked", True)
        with self._make_repo(locked=locked, cache_dir=odb.cache_dir) as repo:
            rev = repo.get_rev()
            if locked and self.def_repo.get(self.PARAM_REV_LOCK) is None:
                self.def_repo[self.PARAM_REV_LOCK] = rev

            path_info = PathInfo(repo.root_dir) / str(self.def_path)
            try:
                imported_objs = repo.used_objs(
                    [os.fspath(path_info)],
                    force=True,
                    jobs=kwargs.get("jobs"),
                    recursive=True,
                )
            except (NoRemoteError, NoOutputOrStageError):
                pass

            def_remote = repo.cloud.get_remote()
            result = [UsedObjectsPair(def_remote, set())]
            for remote, objs in imported_objs:
                if remote is None:
                    result[0].objs.update(objs)
                # else ignore chained imports

            staged_obj = stage(
                odb,
                path_info,
                repo.repo_fs,
                odb.fs.PARAM_CHECKSUM,
            )
            self._staged_objs[rev] = staged_obj
            result[0].objs.add(staged_obj)
            return result

    def get_obj(self, filter_info=None, **kwargs):
        from dvc.objects.stage import stage

        odb = self.repo.odb.local
        locked = kwargs.pop("locked", True)
        with self._make_repo(locked=locked, cache_dir=odb.cache_dir) as repo:
            rev = repo.get_rev()
            if locked and self.def_repo.get(self.PARAM_REV_LOCK) is None:
                self.def_repo[self.PARAM_REV_LOCK] = rev
            obj = self._staged_objs.get(rev)
            if obj is not None:
                return obj

            path_info = PathInfo(repo.root_dir) / str(self.def_path)
            obj = stage(
                odb,
                path_info,
                repo.repo_fs,
                odb.fs.PARAM_CHECKSUM,
            )
            def_remote = repo.cloud.get_remote()
            self._staged_objs[rev] = UsedObjectsPair(obj, def_remote)
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
