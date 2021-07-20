import logging
from typing import TYPE_CHECKING, Dict, Iterable, NamedTuple, Optional, Set

from dvc.hash_info import HashInfo

from .tree import Tree

if TYPE_CHECKING:
    from .db.base import ObjectDB
    from .db.index import ObjectDBIndexBase
    from .file import HashFile

logger = logging.getLogger(__name__)


class StatusResult(NamedTuple):
    exists: Set["HashFile"]
    missing: Set["HashFile"]


class CompareStatusResult(NamedTuple):
    ok: Set["HashFile"]
    missing: Set["HashFile"]
    new: Set["HashFile"]
    deleted: Set["HashFile"]


def _indexed_dir_hashes(odb, index, hash_infos, name, cache_odb):
    from . import load

    # Validate our index by verifying all indexed .dir hashes
    # still exist on the remote
    dir_hashes = {hash_info.value for hash_info in hash_infos}
    indexed_dirs = set(index.dir_hashes())
    indexed_dir_exists = set()
    if indexed_dirs:
        indexed_dir_exists.update(odb.list_hashes_exists(indexed_dirs))
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
    dir_exists.update(odb.list_hashes_exists(dir_hashes - dir_exists))

    # If .dir hash exists in the ODB, assume directory contents
    # also exists
    for dir_hash in dir_exists:
        try:
            tree = load(cache_odb, HashInfo(name, dir_hash))
        except FileNotFoundError:
            continue
        file_hashes = [entry.hash_info.value for _, entry in tree]
        if dir_hash not in index:
            logger.debug(
                "Indexing new .dir '%s' with '%s' nested files",
                dir_hash,
                len(file_hashes),
            )
            index.update([dir_hash], file_hashes)
        yield from file_hashes
        yield tree.hash_info.value


def _status_staging(objs: Iterable["HashFile"]) -> "StatusResult":
    exists: Set["HashFile"] = set()
    for obj in objs:
        if isinstance(obj, Tree):
            exists.update(entry for _, entry in obj)
        exists.add(obj)
    return StatusResult(exists, set())


def status(
    odb: "ObjectDB",
    objs: Iterable["HashFile"],
    name: Optional[str] = None,
    index: Optional["ObjectDBIndexBase"] = None,
    cache_odb: Optional["ObjectDB"] = None,
    **kwargs,
) -> "StatusResult":
    """Return status of whether or not the specified objects exist odb.

    If cache_odb is set, trees will be loaded from cache_odb instead of odb
    when needed.

    Status is returned as a tuple of:
        exists: objs that exist in odb
        missing: objs that do not exist in ODB
    """
    from .stage import is_memfs_staging

    logger.debug("Preparing to collect status from '%s'", odb.path_info)
    if not name:
        name = odb.fs.PARAM_CHECKSUM

    if is_memfs_staging(odb):
        # assume memfs staged objects already exist
        return _status_staging(objs)

    hash_objs: Dict[str, "HashFile"] = {}
    hashes: Set[str] = set()
    dir_infos: Set["HashInfo"] = set()
    exists: Set[str] = set()
    for obj in objs:
        assert obj.hash_info and obj.hash_info.value
        if isinstance(obj, Tree):
            for _, entry in obj:
                assert entry.hash_info and entry.hash_info.value
                hash_objs[entry.hash_info.value] = entry
                hashes.add(entry.hash_info.value)
            if index:
                dir_infos.add(obj.hash_info)
        hashes.add(obj.hash_info.value)
        hash_objs[obj.hash_info.value] = obj

    logger.debug("Collecting status from '%s'", odb.path_info)
    if index and hashes:
        if dir_infos:
            if cache_odb is None:
                cache_odb = odb
            exists = hashes.intersection(
                _indexed_dir_hashes(odb, index, dir_infos, name, cache_odb)
            )
            hashes.difference_update(exists)
        if hashes:
            exists.update(index.intersection(hashes))
            hashes.difference_update(exists)

    if hashes:
        exists.update(
            odb.hashes_exist(hashes, name=str(odb.path_info), **kwargs)
        )
    return StatusResult(
        {hash_objs[hash_] for hash_ in exists},
        {hash_objs[hash_] for hash_ in (hashes - exists)},
    )


def compare_status(
    src: "ObjectDB",
    dest: "ObjectDB",
    objs: Iterable["HashFile"],
    log_missing: bool = True,
    check_deleted: bool = True,
    src_index: Optional["ObjectDBIndexBase"] = None,
    dest_index: Optional["ObjectDBIndexBase"] = None,
    **kwargs,
) -> "CompareStatusResult":
    """Compare status for the specified objects between two ODBs.

    Status is returned as a tuple of:
        ok: hashes that exist in both src and dest
        missing: hashes that do not exist in neither src nor dest
        new: hashes that only exist in src
        deleted: hashes that only exist in dest
    """
    dest_exists, dest_missing = status(dest, objs, index=dest_index, **kwargs)
    # for transfer operations we can skip src status check when all objects
    # already exist in dest
    if dest_missing or check_deleted:
        src_exists, src_missing = status(src, objs, index=src_index, **kwargs)
    else:
        src_exists = dest_exists
        src_missing = set()
    result = CompareStatusResult(
        src_exists & dest_exists,
        src_missing & dest_missing,
        src_exists - dest_exists,
        dest_exists - src_exists,
    )
    if log_missing and result.missing:
        missing_desc = "\n".join(
            f"name: {obj.name}, {obj.hash_info}" for obj in result.missing
        )
        logger.warning(
            "Some of the cache files do not exist neither locally "
            f"nor on remote. Missing cache files:\n{missing_desc}"
        )
    return result
