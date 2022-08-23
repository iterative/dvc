import logging
from collections import defaultdict
from copy import copy
from typing import TYPE_CHECKING, Any, Dict, Optional, Set, Tuple

from voluptuous import Required

from dvc.exceptions import DvcException
from dvc.stage import Stage
from dvc.types import AnyPath
from dvc_objects.fs.base import FileSystem

from .base import Dependency

if TYPE_CHECKING:
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_objects.db import ObjectDB

logger = logging.getLogger(__name__)


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

    def get_used_objs(
        self, **kwargs
    ) -> Dict[Optional["ObjectDB"], Set["HashInfo"]]:
        from dvc_data.build import build
        from dvc_data.objects.tree import Tree, TreeError

        used_obj_ids: Dict[
            Optional["ObjectDB"], Set["HashInfo"]
        ] = defaultdict(set)
        local_odb = self.repo.odb.local
        try:
            object_store, _, obj = build(
                local_odb,
                self.fs_path,
                self.fs,
                local_odb.fs.PARAM_CHECKSUM,
            )
            logger.debug(
                "Staged versioned-import dvc-data reference to '%s://%s'",
                obj.fs.protocol,
                obj.path,
            )
        except (FileNotFoundError, TreeError) as exc:
            raise DvcException(
                f"The path '{self.fs_path}' does not exist in the remote."
            ) from exc
        # TODO: support versioned import writes (push)
        object_store = copy(object_store)
        object_store.read_only = True

        used_obj_ids[object_store].add(obj.hash_info)
        if isinstance(obj, Tree):
            used_obj_ids[object_store].update(oid for _, _, oid in obj)
        return used_obj_ids

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
