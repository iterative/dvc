import logging
from typing import TYPE_CHECKING, Optional

from dvc.path_info import PathInfo

from .file import HashFile

if TYPE_CHECKING:
    from dvc.types import DvcPath

    from .db.base import ObjectDB

logger = logging.getLogger(__name__)


class ExternalRepoFile(HashFile):
    """Lazy erepo object.

    Will be staged in def_odb when object hash is needed.
    """

    def __init__(
        self,
        def_odb: "ObjectDB",
        repo_url: str,
        repo_rev: Optional[str],
        path_info: "DvcPath",
        name: Optional[str] = None,
    ):  # pylint: disable=super-init-not-called
        self.def_odb = def_odb
        self.repo_url = repo_url
        self.repo_rev = repo_rev
        self.path_info = path_info
        self._hash_info = None
        self.name = name

    def __str__(self):
        return f"external object {self.repo_url}@{self.repo_rev}"

    def __bool__(self):
        return bool(self._hash_info)

    def __eq__(self, other):
        if not isinstance(other, ExternalRepoFile):
            return False
        return (
            self.repo_url == other.repo_rev
            and self.path_info == other.path_info
        )

    def __hash__(self):
        return hash((self.repo_url, self.repo_rev, self.path_info))

    def _make_repo(self, **kwargs):
        from dvc.external_repo import external_repo

        return external_repo(self.repo_url, rev=self.repo_rev, **kwargs)

    def _stage(
        self,
        odb: Optional["ObjectDB"] = None,
        fetch: bool = False,
        jobs: int = None,
    ):
        from dvc.config import NoRemoteError
        from dvc.exceptions import NoOutputOrStageError

        from .stage import stage

        odb = odb or self.def_odb
        cache_dir = getattr(odb, "cache_dir", None) if fetch else None

        with self._make_repo(cache_dir=cache_dir) as repo:
            path_info = PathInfo(repo.root_dir) / str(self.path_info)
            if fetch:
                try:
                    repo.fetch([path_info.fspath], jobs=jobs, recursive=True)
                except (NoOutputOrStageError, NoRemoteError):
                    pass
            self.repo_rev = repo.get_rev()
            obj = stage(
                odb,
                path_info,
                repo.repo_fs,
                odb.fs.PARAM_CHECKSUM,
            )
            self._hash_info = obj.hash_info
            return obj

    @property
    def hash_info(self):
        if self._hash_info is None:
            self._stage()
        return self._hash_info

    def fetch_obj(self, odb: "ObjectDB", jobs: int = None):
        return self._stage(odb, fetch=True, jobs=jobs)
