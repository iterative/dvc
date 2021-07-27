import errno
import itertools
import logging
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from functools import partial, wraps
from typing import TYPE_CHECKING, Callable, Iterable, Optional

from funcy import split

from dvc.progress import Tqdm

if TYPE_CHECKING:
    from dvc.hash_info import HashInfo

    from .db.base import ObjectDB
    from .db.index import ObjectDBIndexBase

logger = logging.getLogger(__name__)


def _log_exceptions(func):
    @wraps(func)
    def wrapper(path_info, *args, **kwargs):
        try:
            func(path_info, *args, **kwargs)
            return 0
        except Exception as exc:  # pylint: disable=broad-except
            # NOTE: this means we ran out of file descriptors and there is no
            # reason to try to proceed, as we will hit this error anyways.
            # pylint: disable=no-member
            if isinstance(exc, OSError) and exc.errno == errno.EMFILE:
                raise

            logger.exception("failed to transfer '%s'", path_info)
            return 1

    return wrapper


def _transfer(
    src, dest, dir_ids, file_ids, missing_ids, jobs, verify, move, **kwargs
):
    func = _log_exceptions(dest.add)
    total = len(dir_ids) + len(file_ids)
    if total == 0:
        return 0
    with Tqdm(total=total, unit="file", desc="Transferring") as pbar:
        func = pbar.wrap_fn(func)
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            processor = partial(
                _create_tasks,
                executor,
                jobs,
                func,
                src,
                verify,
                move,
            )
            processor.add_func = func
            _do_transfer(
                src,
                dest,
                dir_ids,
                file_ids,
                missing_ids,
                processor,
                verify=verify,
                move=move,
                **kwargs,
            )
    return total


def _create_tasks(executor, jobs, func, src, verify, move, obj_ids):
    fails = 0
    hash_iter = iter(obj_ids)

    def submit(hash_info):
        obj = src.get(hash_info)
        return executor.submit(
            func,
            obj.path_info,
            obj.fs,
            obj.hash_info,
            verify=verify,
            move=move,
        )

    def create_taskset(amount):
        return {
            submit(hash_info)
            for hash_info in itertools.islice(hash_iter, amount)
        }

    tasks = create_taskset(jobs * 5)
    while tasks:
        done, tasks = futures.wait(tasks, return_when=futures.FIRST_COMPLETED)
        fails += sum(task.result() for task in done)
        tasks.update(create_taskset(len(done)))
    return fails


def _do_transfer(
    src: "ObjectDB",
    dest: "ObjectDB",
    dir_ids: Iterable["HashInfo"],
    file_ids: Iterable["HashInfo"],
    missing_ids: Iterable["HashInfo"],
    processor: Callable,
    src_index: Optional["ObjectDBIndexBase"] = None,
    dest_index: Optional["ObjectDBIndexBase"] = None,
    cache_odb: Optional["ObjectDB"] = None,
    **kwargs,
):
    from dvc.exceptions import FileTransferError
    from dvc.objects.errors import ObjectFormatError

    total_fails = 0
    succeeded_dir_objs = []
    all_file_ids = set(file_ids)

    for dir_hash in dir_ids:
        from .tree import Tree

        bound_file_ids = set()
        dir_obj: Optional["Tree"] = None
        for odb in (cache_odb, src):
            if odb is not None:
                try:
                    dir_obj = Tree.load(odb, dir_hash)
                    break
                except (FileNotFoundError, ObjectFormatError):
                    pass
        assert dir_obj
        entry_ids = {entry.hash_info for _, entry in dir_obj}

        for file_hash in all_file_ids.copy():
            if file_hash in entry_ids:
                bound_file_ids.add(file_hash)
                all_file_ids.remove(file_hash)

        dir_fails = processor(bound_file_ids)
        if dir_fails:
            logger.debug(
                "failed to upload full contents of '%s', "
                "aborting .dir file upload",
                dir_obj.name,
            )
            logger.error(
                "failed to upload '%s' to '%s'",
                src.get(dir_obj.hash_info).path_info,
                dest.get(dir_obj.hash_info).path_info,
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
            raw_obj = src.get(dir_obj.hash_info)
            is_dir_failed = processor.add_func(  # type: ignore[attr-defined]
                raw_obj.path_info,
                raw_obj.fs,
                raw_obj.hash_info,
                **kwargs,
            )
            total_fails += is_dir_failed
            if not is_dir_failed:
                succeeded_dir_objs.append(dir_obj)

    # insert the rest
    total_fails += processor(all_file_ids)
    if total_fails:
        if src_index:
            src_index.clear()
        raise FileTransferError(total_fails)

    # index successfully pushed dirs
    if dest_index:
        for dir_obj in succeeded_dir_objs:
            file_hashes = {entry.hash_info.value for _, entry in dir_obj}
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
    move: bool = False,
    **kwargs,
) -> int:
    """Transfer (copy) the specified objects from one ODB to another.

    Returns the number of successfully transferred objects
    """
    from .status import compare_status

    logger.debug(
        "Preparing to transfer data from '%s' to '%s'",
        src.path_info,
        dest.path_info,
    )
    if src == dest:
        return 0

    status = compare_status(src, dest, obj_ids, check_deleted=False, **kwargs)
    if not status.new:
        return 0

    dir_ids, file_ids = split(lambda hash_info: hash_info.isdir, status.new)
    if jobs is None:
        jobs = dest.fs.jobs

    return _transfer(
        src,
        dest,
        set(dir_ids),
        set(file_ids),
        status.missing,
        jobs,
        verify,
        move,
        **kwargs,
    )
