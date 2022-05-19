from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .index import ObjectDBIndexBase


def get_odb(fs, fs_path, **config):
    from dvc_objects.db import ObjectDB
    from dvc_objects.fs import Schemes

    from .local import LocalObjectDB

    if fs.protocol == Schemes.LOCAL:
        return LocalObjectDB(fs, fs_path, **config)

    return ObjectDB(fs, fs_path, **config)


def get_index(odb) -> "ObjectDBIndexBase":
    import hashlib

    from .index import ObjectDBIndex, ObjectDBIndexNoop

    cls = ObjectDBIndex if odb.tmp_dir else ObjectDBIndexNoop
    return cls(
        odb.tmp_dir,
        hashlib.sha256(
            odb.fs.unstrip_protocol(odb.fs_path).encode("utf-8")
        ).hexdigest(),
    )
