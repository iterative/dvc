import errno
import logging
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

from funcy import split

from dvc.progress import Tqdm
from dvc.utils.threadpool import ThreadPoolExecutor

if TYPE_CHECKING:
    from dvc.hash_info import HashInfo
    from dvc.objects.db import ObjectDB

    from .db.index import ObjectDBIndexBase
    from .tree import Tree

logger = logging.getLogger(__name__)


def _log_exceptions(func):
    @wraps(func)
    def wrapper(fs_path, *args, **kwargs):
        try:
            func(fs_path, *args, **kwargs)
            return 0
        except Exception as exc:  # pylint: disable=broad-except
            # NOTE: this means we ran out of file descriptors and there is no
            # reason to try to proceed, as we will hit this error anyways.
            # pylint: disable=no-member
            if isinstance(exc, OSError) and exc.errno == errno.EMFILE:
                raise

            logger.exception("failed to transfer '%s'", fs_path)
            return 1

    return wrapper


def find_tree_by_obj_id(
    odbs: Iterable[Optional["ObjectDB"]], obj_id: "HashInfo"
) -> Optional["Tree"]:
    from dvc.objects.errors import ObjectFormatError

    from .tree import Tree

    for odb in odbs:
        if odb is not None:
            try:
                return Tree.load(odb, obj_id)
            except (FileNotFoundError, ObjectFormatError):
                pass
    return None


def _do_transfer(
    src: "ObjectDB",
    dest: "ObjectDB",
    obj_ids: Iterable["HashInfo"],
    missing_ids: Iterable["HashInfo"],
    processor: Callable,
    src_index: Optional["ObjectDBIndexBase"] = None,
    dest_index: Optional["ObjectDBIndexBase"] = None,
    cache_odb: Optional["ObjectDB"] = None,
    **kwargs: Any,
):
    from dvc.exceptions import FileTransferError

    dir_ids, file_ids = split(lambda hash_info: hash_info.isdir, obj_ids)
    total_fails = 0
    succeeded_dir_objs = []
    all_file_ids = set(file_ids)

    for dir_hash in dir_ids:
        dir_obj = find_tree_by_obj_id([cache_odb, src], dir_hash)
        assert dir_obj

        entry_ids = {oid for _, _, oid in dir_obj}
        bound_file_ids = all_file_ids & entry_ids
        all_file_ids -= entry_ids

        dir_fails = sum(processor(bound_file_ids))
        if dir_fails:
            logger.debug(
                "failed to upload full contents of '%s', "
                "aborting .dir file upload",
                dir_obj.name,
            )
            logger.error(
                "failed to upload '%s' to '%s'",
                src.get(dir_obj.hash_info).fs_path,
                dest.get(dir_obj.hash_info).fs_path,
            )
            total_fails += dir_fails + 1
        elif entry_ids.intersection(missing_ids):
            # if for some reason a file contained in this dir is
            # missing both locally and in the remote, we want to
            # push whatever file content we have, but should not
            # push .dir file
            logger.debug(
                "directory '%s' contains missing files,"
                "skipping .dir file upload",
                dir_obj.name,
            )
        else:
            is_dir_failed = sum(processor([dir_obj.hash_info]))
            total_fails += is_dir_failed
            if not is_dir_failed:
                succeeded_dir_objs.append(dir_obj)

    # insert the rest
    total_fails += sum(processor(all_file_ids))
    if total_fails:
        if src_index:
            src_index.clear()
        raise FileTransferError(total_fails)

    # index successfully pushed dirs
    if dest_index:
        for dir_obj in succeeded_dir_objs:
            file_hashes = {oid.value for _, _, oid in dir_obj}
            logger.debug(
                "Indexing pushed dir '%s' with '%s' nested files",
                dir_obj.hash_info,
                len(file_hashes),
            )
            assert dir_obj.hash_info and dir_obj.hash_info.value
            dest_index.update([dir_obj.hash_info.value], file_hashes)


def transfer(
    src: "ObjectDB",
    dest: "ObjectDB",
    obj_ids: Iterable["HashInfo"],
    jobs: Optional[int] = None,
    verify: bool = False,
    hardlink: bool = False,
    **kwargs,
) -> int:
    """Transfer (copy) the specified objects from one ODB to another.

    Returns the number of successfully transferred objects
    """
    from .status import compare_status

    logger.debug(
        "Preparing to transfer data from '%s' to '%s'",
        src.fs_path,
        dest.fs_path,
    )
    if src == dest:
        return 0

    status = compare_status(
        src, dest, obj_ids, check_deleted=False, jobs=jobs, **kwargs
    )
    if not status.new:
        return 0

    def func(hash_info: "HashInfo") -> None:
        obj = src.get(hash_info)
        return dest.add(
            obj.fs_path,
            obj.fs,
            obj.hash_info,
            verify=verify,
            hardlink=hardlink,
        )

    total = len(status.new)
    jobs = jobs or dest.fs.jobs
    with Tqdm(total=total, unit="file", desc="Transferring") as pbar:
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            wrapped_func = pbar.wrap_fn(_log_exceptions(func))
            processor = partial(executor.imap_unordered, wrapped_func)
            _do_transfer(
                src, dest, status.new, status.missing, processor, **kwargs
            )
    return total
