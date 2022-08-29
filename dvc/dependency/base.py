from collections import defaultdict
from copy import copy
from typing import TYPE_CHECKING, Dict, Optional, Set, Type

from dvc.exceptions import DvcException
from dvc.output import Output
from dvc_data.hashfile.meta import Meta

if TYPE_CHECKING:
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_objects.db import ObjectDB


class DependencyDoesNotExistError(DvcException):
    def __init__(self, path):
        msg = f"dependency '{path}' does not exist"
        super().__init__(msg)


class DependencyIsNotFileOrDirError(DvcException):
    def __init__(self, path):
        msg = f"dependency '{path}' is not a file or directory"
        super().__init__(msg)


class DependencyIsStageFileError(DvcException):
    def __init__(self, path):
        super().__init__(f"DVC file '{path}' cannot be a dependency.")


class Dependency(Output):
    IS_DEPENDENCY = True

    DoesNotExistError = DependencyDoesNotExistError  # type: Type[DvcException]
    IsNotFileOrDirError = (
        DependencyIsNotFileOrDirError
    )  # type: Type[DvcException]
    IsStageFileError = DependencyIsStageFileError  # type: Type[DvcException]

    def get_used_objs(
        self, **kwargs
    ) -> Dict[Optional["ObjectDB"], Set["HashInfo"]]:
        from dvc_data.build import build
        from dvc_data.objects.tree import Tree, TreeError

        if not self.meta.version_id:
            return super().get_used_objs(**kwargs)

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
        if not self.meta.version_id:
            return super().workspace_status()

        current = self.meta.version_id
        fs_path = self.fs.path.version_path(self.fs_path, None)
        updated = self.fs.info(fs_path)[Meta.PARAM_VERSION_ID]

        if current != updated:
            return {str(self): "update available"}

        return {}

    def status(self):
        if not self.meta.version_id:
            return super().status()

        return self.workspace_status()

    def update(self, rev: Optional[str] = None):
        if not self.meta.version_id:
            return

        if rev:
            self.meta.version_id = rev
        else:
            fs_path = self.fs.path.version_path(self.fs_path, rev)
            details = self.fs.info(fs_path)
            self.meta.version_id = details[Meta.PARAM_VERSION_ID]
        self.fs_path = self.fs.path.version_path(
            self.fs_path, self.meta.version_id
        )
