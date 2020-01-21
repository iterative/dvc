import os

from voluptuous import Required

from .local import DependencyLOCAL
from dvc.exceptions import OutputNotFoundError
from dvc.path_info import PathInfo


class DependencyREPO(DependencyLOCAL):
    PARAM_REPO = "repo"
    PARAM_URL = "url"
    PARAM_REV = "rev"
    PARAM_REV_LOCK = "rev_lock"

    REPO_SCHEMA = {
        Required(PARAM_URL): str,
        PARAM_REV: str,
        PARAM_REV_LOCK: str,
    }

    def __init__(self, def_repo, stage, *args, **kwargs):
        self.def_repo = def_repo
        super().__init__(stage, *args, **kwargs)

    def _parse_path(self, remote, path):
        return None

    @property
    def is_in_repo(self):
        return False

    @property
    def repo_pair(self):
        d = self.def_repo
        rev = d.get(self.PARAM_REV_LOCK) or d.get(self.PARAM_REV)
        return d[self.PARAM_URL], rev

    def __str__(self):
        return "{} ({})".format(self.def_path, self.def_repo[self.PARAM_URL])

    def _make_repo(self, *, locked=True):
        from dvc.external_repo import external_repo

        d = self.def_repo
        rev = (d.get("rev_lock") if locked else None) or d.get("rev")
        return external_repo(d["url"], rev=rev)

    def _get_checksum(self, locked=True):
        with self._make_repo(locked=locked) as repo:
            try:
                return repo.find_out_by_relpath(self.def_path).info["md5"]
            except OutputNotFoundError:
                path = PathInfo(os.path.join(repo.root_dir, self.def_path))
                # We are polluting our repo cache with some dir listing here
                return self.repo.cache.local.get_checksum(path)

    def status(self):
        current_checksum = self._get_checksum(locked=True)
        updated_checksum = self._get_checksum(locked=False)

        if current_checksum != updated_checksum:
            return {str(self): "update available"}

        return {}

    def save(self):
        pass

    def dumpd(self):
        return {self.PARAM_PATH: self.def_path, self.PARAM_REPO: self.def_repo}

    def download(self, to):
        with self._make_repo() as repo:
            if self.def_repo.get(self.PARAM_REV_LOCK) is None:
                self.def_repo[self.PARAM_REV_LOCK] = repo.scm.get_rev()

            if hasattr(repo, "cache"):
                repo.cache.local.cache_dir = self.repo.cache.local.cache_dir

            repo.pull_to(self.def_path, to.path_info)

    def update(self):
        with self._make_repo(locked=False) as repo:
            self.def_repo[self.PARAM_REV_LOCK] = repo.scm.get_rev()
