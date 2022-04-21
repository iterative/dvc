from typing import TYPE_CHECKING, Tuple, Union

from .stage import stage
from .transfer import transfer
from .tree import Tree

if TYPE_CHECKING:
    from dvc.data.meta import Meta
    from dvc.fs.base import FileSystem
    from dvc.ignore import DvcIgnore
    from dvc.objects.db import ObjectDB
    from dvc.objects.file import HashFile
    from dvc.types import AnyPath


def add(
    odb: "ObjectDB",
    fs: "FileSystem",
    fs_path: "AnyPath",
    name: str,
    filter_prefix: Union["AnyPath", Tuple[str, ...]] = None,
    hardlink: bool = False,
    dvcignore: "DvcIgnore" = None,
    upload: bool = False,
    jobs: int = None,
    no_progress_bar: bool = False,
) -> Tuple["Meta", Union["HashFile", "Tree"]]:
    staging, meta, obj = stage(
        odb,
        fs_path,
        fs,
        name,
        upload=upload,
        jobs=jobs,
        dvcignore=dvcignore,
        no_progress_bar=no_progress_bar,
    )

    obj_ids = {obj.hash_info}

    if filter_prefix is None or isinstance(filter_prefix, tuple):
        prefix = filter_prefix
    else:
        prefix = fs.path.parts(fs.path.relpath(filter_prefix, fs_path))

    if prefix and isinstance(obj, Tree):
        obj = obj.filter(prefix)
        obj_ids.update({oid for _, _, oid in obj})

    transfer(
        staging,
        odb,
        obj_ids,
        shallow=bool(prefix),
        hardlink=hardlink,
        jobs=jobs,
    )
    return meta, obj
