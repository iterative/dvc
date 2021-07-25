import errno
import itertools
import logging
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from functools import partial, wraps
from typing import TYPE_CHECKING, Iterable, Optional, Set

from dvc.progress import Tqdm

from .file import HashFile
from .tree import Tree

if TYPE_CHECKING:
    from .db.base import ObjectDB
    from .db.index import ObjectDBIndexBase

logger = logging.getLogger(__name__)


def _log_exceptions(func):
    @wraps(func)
    def wrapper(odb, obj, *args, **kwargs):
        try:
            func(odb, obj, *args, **kwargs)
            return 0
        except Exception as exc:  # pylint: disable=broad-except
            # NOTE: this means we ran out of file descriptors and there is no
            # reason to try to proceed, as we will hit this error anyways.
            # pylint: disable=no-member
            if isinstance(exc, OSError) and exc.errno == errno.EMFILE:
                raise

            logger.exception(
                "failed to transfer '%s' to '%s'",
                obj.path_info,
                odb.get(obj.hash_info).path_info,
            )
            return 1

    return wrapper


def _transfer(src, dest, dir_objs, file_objs, missing, jobs, verify, **kwargs):
    from . import save
    from .stage import is_memfs_staging

    is_staged = is_memfs_staging(src)
    func = _log_exceptions(save)
    total = len(dir_objs) + len(file_objs)
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
                dest,
                is_staged,
                verify,
            )
            processor.save_func = func
            _do_transfer(
                src,
                dest,
                dir_objs,
                file_objs,
                {obj.hash_info for obj in missing},
                processor,
                **kwargs,
            )
    return total


def _create_tasks(executor, jobs, func, src, dest, is_staged, verify, objs):
    fails = 0
    obj_iter = iter(objs)

    def create_taskset(amount):
        return {
            executor.submit(
                func,
                dest,
                _raw_obj(src, obj, is_staged),
                move=False,
                verify=verify,
            )
            for obj in itertools.islice(obj_iter, amount)
        }

    tasks = create_taskset(jobs * 5)
    while tasks:
        done, tasks = futures.wait(tasks, return_when=futures.FIRST_COMPLETED)
        fails += sum(task.result() for task in done)
        tasks.update(create_taskset(len(done)))
    return fails


def _raw_obj(odb, obj, is_staged=False):
    if is_staged:
        return HashFile(obj.path_info, obj.fs, obj.hash_info)
    return odb.get(obj.hash_info)


def _do_transfer(
    src,
    dest,
    dir_objs,
    file_objs,
    missing_hashes,
    processor,
    src_index: Optional["ObjectDBIndexBase"] = None,
    dest_index: Optional["ObjectDBIndexBase"] = None,
    **kwargs,
):
    from dvc.exceptions import FileTransferError

    total_fails = 0
    succeeded_dir_objs = []
    all_file_objs = set(file_objs)

    for dir_obj in dir_objs:
        bound_file_objs = set()
        directory_hashes = {entry.hash_info for _, entry in dir_obj}

        for file_obj in all_file_objs.copy():
            if file_obj.hash_info in directory_hashes:
                bound_file_objs.add(file_obj)
                all_file_objs.remove(file_obj)

        dir_fails = processor(bound_file_objs)
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
        elif directory_hashes.intersection(missing_hashes):
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
            is_dir_failed = processor.save_func(
                dest, src.get(dir_obj.hash_info), move=False
            )
            total_fails += is_dir_failed
            if not is_dir_failed:
                succeeded_dir_objs.append(dir_obj)

    # insert the rest
    total_fails += processor(all_file_objs)
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
            dest_index.update([dir_obj.hash_info.value], file_hashes)


def transfer(
    src: "ObjectDB",
    dest: "ObjectDB",
    objs: Iterable["HashFile"],
    jobs: Optional[int] = None,
    verify: bool = False,
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
    status = compare_status(src, dest, objs, check_deleted=False, **kwargs)
    if not status.new:
        return 0

    files: Set["HashFile"] = set()
    dirs: Set["HashFile"] = set()
    for obj in status.new:
        if isinstance(obj, Tree):
            dirs.add(obj)
        else:
            files.add(obj)
    if jobs is None:
        jobs = dest.fs.jobs

    return _transfer(
        src,
        dest,
        dirs,
        files,
        status.missing,
        jobs,
        verify,
        **kwargs,
    )
