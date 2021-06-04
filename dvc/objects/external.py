import logging
from typing import TYPE_CHECKING, Optional

from dvc.path_info import PathInfo

from .errors import ObjectFormatError
from .file import HashFile

if TYPE_CHECKING:
    from dvc.hash_info import HashInfo
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
        self._hash_info: Optional["HashInfo"] = None
        self.name = name

    def __str__(self):
        return (
            f"external object {self.repo_url}: {self.path_info} "
            f"@ {self.repo_rev}"
        )

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
        from dvc.scm.base import CloneError

        try:
            return external_repo(self.repo_url, rev=self.repo_rev, **kwargs)
        except CloneError as exc:
            raise ObjectFormatError(
                f"Could not clone external obj repo '{self.repo_url}'"
            ) from exc

    def get_staged(self, odb: Optional["ObjectDB"] = None):
        from . import load
        from .stage import stage

        odb = odb or self.def_odb

        if self._hash_info is not None:
            try:
                odb.check(self._hash_info, False)
                return load(odb, self._hash_info)
            except (FileNotFoundError, ObjectFormatError):
                pass

        cache_dir = getattr(odb, "cache_dir", None)
        with self._make_repo(cache_dir=cache_dir) as repo:
            path_info = PathInfo(repo.root_dir) / str(self.path_info)
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
            self.get_staged()
        return self._hash_info

    @property
    def latest_rev(self):
        with self._make_repo(locked=False) as repo:
            return repo.get_rev()
