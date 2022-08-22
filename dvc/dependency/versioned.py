from typing import Any, Dict, Optional, Tuple

from voluptuous import Required

from dvc.exceptions import DvcException
from dvc.stage import Stage
from dvc.types import AnyPath
from dvc_objects.fs.base import FileSystem

from .base import Dependency


def _coalesce_version_id(
    fs: FileSystem, path: AnyPath, version_id: Optional[str]
) -> Tuple[AnyPath, Optional[str]]:
    path, path_version_id = fs.path.split_version(path)
    versions = {ver for ver in (version_id, path_version_id) if ver}
    if len(versions) > 1:
        raise DvcException(
            "Specified file versions do not match: '{path}', '{version_id}'"
        )
    return path, (versions.pop() if versions else None)


class VersionedDependency(Dependency):
    PARAM_VERSION_ID = "version_id"

    VERSION_SCHEMA = {
        Required(PARAM_VERSION_ID): str,
    }

    def __init__(
        self,
        stage: Stage,
        path: AnyPath,
        version_id: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(stage, path, **kwargs)
        assert self.fs.fs.version_aware
        self.def_path, self.version_id = _coalesce_version_id(
            self.fs, self.def_path, version_id
        )
        self.fs_path = self.fs.path.version_path(self.fs_path, self.version_id)

    def __str__(self):
        return f"{self.def_path} ({self.version_id})"

    def workspace_status(self):
        current = self.version_id
        fs_path = self.fs.path.version_path(self.fs_path, None)
        updated = self.fs.info(fs_path)["version_id"]

        if current != updated:
            return {str(self): "update available"}

        return {}

    def status(self):
        return self.workspace_status()

    def update(self, rev: Optional[str] = None):
        """Update dependency to the specified version.

        Arguments:
            rev: Version ID. rev=None will update to the latest file
                version.
        """
        if rev:
            self.version_id = rev
        else:
            fs_path = self.fs.path.version_path(self.fs_path, rev)
            details = self.fs.info(fs_path)
            self.version_id = details["version_id"]
        self.fs_path = self.fs.path.version_path(self.fs_path, self.version_id)

    def dumpd(self) -> Dict[str, Any]:
        assert self.version_id is not None
        ret = super().dumpd()
        ret[self.PARAM_VERSION_ID] = self.version_id
        return ret
