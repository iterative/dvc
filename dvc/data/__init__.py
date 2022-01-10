import logging
from typing import TYPE_CHECKING, Iterator, Union

from .tree import Tree

if TYPE_CHECKING:
    from dvc.hash_info import HashInfo
    from dvc.objects.db import ObjectDB
    from dvc.objects.file import HashFile

logger = logging.getLogger(__name__)


def check(odb: "ObjectDB", obj: "HashFile", **kwargs):
    if isinstance(obj, Tree):
        for _, _, oid in obj:
            odb.check(oid, **kwargs)

    odb.check(obj.hash_info, **kwargs)


def load(odb: "ObjectDB", hash_info: "HashInfo") -> "HashFile":
    if hash_info.isdir:
        return Tree.load(odb, hash_info)
    return odb.get(hash_info)


def iterobjs(
    obj: Union["Tree", "HashFile"]
) -> Iterator[Union["Tree", "HashFile"]]:
    if isinstance(obj, Tree):
        yield from (entry_obj for _, entry_obj in obj)
    yield obj
