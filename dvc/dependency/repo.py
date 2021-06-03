from voluptuous import Required

from .base import Dependency


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
        from dvc.objects import save

        odb = self.repo.odb.local

        obj = self.get_obj()
        save(odb, obj, jobs=jobs)
        if self.def_repo.get(self.PARAM_REV_LOCK) is None:
            self.def_repo[self.PARAM_REV_LOCK] = obj.repo_rev

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

    def get_obj(
        self, locked=True, **kwargs
    ):  # pylint: disable=arguments-differ
        from dvc.objects.external import ExternalRepoFile

        d = self.def_repo
        rev = (d.get(self.PARAM_REV_LOCK) if locked else None) or d.get(
            self.PARAM_REV
        )
        return ExternalRepoFile(
            self.repo.odb.local, d[self.PARAM_URL], rev, self.def_path
        )
