import logging
from typing import TYPE_CHECKING, Dict, Iterable, NamedTuple, Optional, Set

from dvc_objects.fs import Schemes
from dvc_objects.hash_info import HashInfo

from .tree import Tree

if TYPE_CHECKING:
    from dvc_objects.db import ObjectDB
    from dvc_objects.file import HashFile

    from .db.index import ObjectDBIndexBase

logger = logging.getLogger(__name__)


class StatusResult(NamedTuple):
    exists: Set["HashInfo"]
    missing: Set["HashInfo"]


class CompareStatusResult(NamedTuple):
    ok: Set["HashInfo"]
    missing: Set["HashInfo"]
    new: Set["HashInfo"]
    deleted: Set["HashInfo"]


def _indexed_dir_hashes(odb, index, dir_objs, name, cache_odb, jobs=None):
    # Validate our index by verifying all indexed .dir hashes
    # still exist on the remote
    from ._progress import QueryingProgress

    dir_hashes = set(dir_objs.keys())
    indexed_dirs = set(index.dir_hashes())
    indexed_dir_exists = set()
    if indexed_dirs:
        hashes = QueryingProgress(
            odb.list_hashes_exists(indexed_dirs, jobs=jobs),
            total=len(indexed_dirs),
        )
        indexed_dir_exists.update(hashes)
        missing_dirs = indexed_dirs.difference(indexed_dir_exists)
        if missing_dirs:
            logger.debug(
                "Remote cache missing indexed .dir hashes '%s', "
                "clearing remote index",
                ", ".join(missing_dirs),
            )
            index.clear()

    # Check if non-indexed (new) dir hashes exist on remote
    dir_exists = dir_hashes.intersection(indexed_dir_exists)
    dir_missing = dir_hashes - dir_exists
    dir_exists.update(
        QueryingProgress(
            odb.list_hashes_exists(dir_missing, jobs=jobs),
            total=len(dir_missing),
        )
    )

    # If .dir hash exists in the ODB, assume directory contents
    # also exists
    for dir_hash in dir_exists:
        tree = dir_objs.get(dir_hash)
        if not tree:
            try:
                tree = Tree.load(cache_odb, HashInfo(name, dir_hash))
            except FileNotFoundError:
                continue
        file_hashes = [oid.value for _, _, oid in tree]
        if dir_hash not in index:
            logger.debug(
                "Indexing new .dir '%s' with '%s' nested files",
                dir_hash,
                len(file_hashes),
            )
            index.update([dir_hash], file_hashes)
        yield from file_hashes
        yield tree.hash_info.value


def status(
    odb: "ObjectDB",
    obj_ids: Iterable["HashInfo"],
    name: Optional[str] = None,
    index: Optional["ObjectDBIndexBase"] = None,
    cache_odb: Optional["ObjectDB"] = None,
    shallow: bool = True,
    jobs: Optional[int] = None,
    **kwargs,
) -> "StatusResult":
    """Return status of whether or not the specified objects exist odb.

    If cache_odb is set, trees will be loaded from cache_odb instead of odb
    when needed.

    Status is returned as a tuple of:
        exists: objs that exist in odb
        missing: objs that do not exist in ODB
    """
    logger.debug("Preparing to collect status from '%s'", odb.fs_path)
    if not name:
        name = odb.fs.PARAM_CHECKSUM

    if cache_odb is None:
        cache_odb = odb

    hash_infos: Dict[str, "HashInfo"] = {}
    dir_objs: Dict[str, Optional["HashFile"]] = {}
    for hash_info in obj_ids:
        assert hash_info.value
        if hash_info.isdir:
            if shallow:
                tree = None
            else:
                tree = Tree.load(cache_odb, hash_info)
                for _, _, oid in tree:
                    assert oid and oid.value
                    hash_infos[oid.value] = oid
            if index:
                dir_objs[hash_info.value] = tree
        hash_infos[hash_info.value] = hash_info

    if odb.fs.protocol == Schemes.MEMORY:
        # assume memfs staged objects already exist
        return StatusResult(set(hash_infos.values()), set())

    hashes: Set[str] = set(hash_infos.keys())
    exists: Set[str] = set()

    logger.debug("Collecting status from '%s'", odb.fs_path)
    if index and hashes:
        if dir_objs:
            exists = hashes.intersection(
                _indexed_dir_hashes(
                    odb, index, dir_objs, name, cache_odb, jobs=jobs
                )
            )
            hashes.difference_update(exists)
        if hashes:
            exists.update(index.intersection(hashes))
            hashes.difference_update(exists)

    if hashes:
        from ._progress import QueryingProgress

        with QueryingProgress(phase="Checking", name=odb.fs_path) as pbar:
            exists.update(
                odb.hashes_exist(hashes, jobs=jobs, progress=pbar.callback)
            )
    return StatusResult(
        {hash_infos[hash_] for hash_ in exists},
        {hash_infos[hash_] for hash_ in (hashes - exists)},
    )


def compare_status(
    src: "ObjectDB",
    dest: "ObjectDB",
    obj_ids: Iterable["HashInfo"],
    check_deleted: bool = True,
    src_index: Optional["ObjectDBIndexBase"] = None,
    dest_index: Optional["ObjectDBIndexBase"] = None,
    jobs: Optional[int] = None,
    **kwargs,
) -> "CompareStatusResult":
    """Compare status for the specified objects between two ODBs.

    Status is returned as a tuple of:
        ok: hashes that exist in both src and dest
        missing: hashes that do not exist in neither src nor dest
        new: hashes that only exist in src
        deleted: hashes that only exist in dest
    """
    if "cache_odb" not in kwargs:
        kwargs["cache_odb"] = src
    dest_exists, dest_missing = status(
        dest, obj_ids, index=dest_index, jobs=jobs, **kwargs
    )
    # for transfer operations we can skip src status check when all objects
    # already exist in dest
    if dest_missing or check_deleted:
        src_exists, src_missing = status(
            src, obj_ids, index=src_index, jobs=jobs, **kwargs
        )
    else:
        src_exists = dest_exists
        src_missing = set()
    return CompareStatusResult(
        src_exists & dest_exists,
        src_missing & dest_missing,
        src_exists - dest_exists,
        dest_exists - src_exists,
    )
