import logging
from collections import defaultdict
from copy import copy
from typing import TYPE_CHECKING, Dict, Optional, Set

from dvc.exceptions import DvcException

from .base import Dependency

if TYPE_CHECKING:
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_objects.db import ObjectDB

logger = logging.getLogger(__name__)


class VersionedDependency(Dependency):
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
