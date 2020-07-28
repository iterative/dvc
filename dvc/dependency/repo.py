import os

from voluptuous import Required

from dvc.path_info import PathInfo

from .local import LocalDependency


class RepoDependency(LocalDependency):
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
        super().__init__(stage, *args, **kwargs)

    def _parse_path(self, tree, path):
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

    def _make_repo(self, *, locked=True, **kwargs):
        from dvc.external_repo import external_repo

        d = self.def_repo
        rev = (d.get("rev_lock") if locked else None) or d.get("rev")
        return external_repo(d["url"], rev=rev, **kwargs)

    def _get_checksum(self, locked=True):
        cache = self.repo.cache.local
        with self._make_repo(
            locked=locked, stream=True, cache_dir=cache.cache_dir
        ) as repo:
            path = PathInfo(os.path.join(repo.root_dir, self.def_path))
            return repo.get_checksum(path, cache)

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
        cache = self.repo.cache.local
        with self._make_repo(cache_dir=cache.cache_dir) as repo:
            if self.def_repo.get(self.PARAM_REV_LOCK) is None:
                self.def_repo[self.PARAM_REV_LOCK] = repo.get_rev()
            _, _, cache_infos = repo.fetch_external([self.def_path], cache)
            cache.checkout(to.path_info, cache_infos[0])

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
