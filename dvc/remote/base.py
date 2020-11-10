import errno
import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial, wraps

from funcy import concat

from dvc.exceptions import DownloadError, UploadError
from dvc.hash_info import HashInfo

from ..progress import Tqdm
from .index import RemoteIndex, RemoteIndexNoop

logger = logging.getLogger(__name__)

STATUS_OK = 1
STATUS_MISSING = 2
STATUS_NEW = 3
STATUS_DELETED = 4

STATUS_MAP = {
    # (local_exists, remote_exists)
    (True, True): STATUS_OK,
    (False, False): STATUS_MISSING,
    (True, False): STATUS_NEW,
    (False, True): STATUS_DELETED,
}


def _log_exceptions(func, operation):
    @wraps(func)
    def wrapper(from_info, to_info, *args, **kwargs):
        try:
            func(from_info, to_info, *args, **kwargs)
            return 0
        except Exception as exc:  # pylint: disable=broad-except
            # NOTE: this means we ran out of file descriptors and there is no
            # reason to try to proceed, as we will hit this error anyways.
            # pylint: disable=no-member
            if isinstance(exc, OSError) and exc.errno == errno.EMFILE:
                raise

            logger.exception(
                "failed to %s '%s' to '%s'", operation, from_info, to_info
            )
            return 1

    return wrapper


def index_locked(f):
    @wraps(f)
    def wrapper(obj, *args, **kwargs):
        with obj.index:
            return f(obj, *args, **kwargs)

    return wrapper


class Remote:
    """Cloud remote class.

    Provides methods for indexing and garbage collecting trees which contain
    DVC remotes.
    """

    INDEX_CLS = RemoteIndex

    def __init__(self, tree):
        from dvc.cache import get_cloud_cache

        self.tree = tree
        self.repo = tree.repo
        self.cache = get_cloud_cache(self.tree)

        config = tree.config
        url = config.get("url")
        if url:
            index_name = hashlib.sha256(url.encode("utf-8")).hexdigest()
            self.index = self.INDEX_CLS(
                self.repo, index_name, dir_suffix=self.tree.CHECKSUM_DIR_SUFFIX
            )
        else:
            self.index = RemoteIndexNoop()

    def __repr__(self):
        return "{class_name}: '{path_info}'".format(
            class_name=type(self).__name__,
            path_info=self.tree.path_info or "No path",
        )

    @index_locked
    def gc(self, *args, **kwargs):
        removed = self.cache.gc(*args, **kwargs)

        if removed:
            self.index.clear()

        return removed

    @index_locked
    def status(
        self,
        cache,
        named_cache,
        jobs=None,
        show_checksums=False,
        download=False,
        log_missing=True,
    ):
        # Return flattened dict containing all status info
        dir_status, file_status, _ = self._status(
            cache,
            named_cache,
            jobs=jobs,
            show_checksums=show_checksums,
            download=download,
            log_missing=log_missing,
        )
        return dict(dir_status, **file_status)

    def hashes_exist(self, hashes, **kwargs):
        hashes = set(hashes)
        indexed_hashes = set(self.index.intersection(hashes))
        hashes -= indexed_hashes
        indexed_hashes = list(indexed_hashes)
        logger.debug("Matched '{}' indexed hashes".format(len(indexed_hashes)))
        if not hashes:
            return indexed_hashes

        return indexed_hashes + self.cache.hashes_exist(list(hashes), **kwargs)

    def _status(
        self,
        cache,
        named_cache,
        jobs=None,
        show_checksums=False,
        download=False,
        log_missing=True,
    ):
        """Return a tuple of (dir_status_info, file_status_info, dir_contents).

        dir_status_info contains status for .dir files, file_status_info
        contains status for all other files, and dir_contents is a dict of
        {dir_hash: set(file_hash, ...)} which can be used to map
        a .dir file to its file contents.
        """
        logger.debug(f"Preparing to collect status from {self.tree.path_info}")
        md5s = set(named_cache.scheme_keys(cache.tree.scheme))

        logger.debug("Collecting information from local cache...")
        local_exists = frozenset(
            cache.hashes_exist(md5s, jobs=jobs, name=cache.cache_dir)
        )

        # This is a performance optimization. We can safely assume that,
        # if the resources that we want to fetch are already cached,
        # there's no need to check the remote storage for the existence of
        # those files.
        if download and local_exists == md5s:
            remote_exists = local_exists
        else:
            logger.debug("Collecting information from remote cache...")
            remote_exists = set()
            dir_md5s = set(named_cache.dir_keys(cache.tree.scheme))
            if dir_md5s:
                remote_exists.update(
                    self._indexed_dir_hashes(cache, named_cache, dir_md5s)
                )
                md5s.difference_update(remote_exists)
            if md5s:
                remote_exists.update(
                    self.hashes_exist(
                        md5s, jobs=jobs, name=str(self.tree.path_info)
                    )
                )
        return self._make_status(
            cache,
            named_cache,
            show_checksums,
            local_exists,
            remote_exists,
            log_missing,
        )

    def _make_status(
        self,
        cache,
        named_cache,
        show_checksums,
        local_exists,
        remote_exists,
        log_missing,
    ):
        def make_names(hash_, names):
            return {"name": hash_ if show_checksums else " ".join(names)}

        dir_status = {}
        file_status = {}
        dir_contents = {}
        for hash_, item in named_cache[cache.tree.scheme].items():
            if item.children:
                dir_status[hash_] = make_names(hash_, item.names)
                dir_contents[hash_] = set()
                for child_hash, child in item.children.items():
                    file_status[child_hash] = make_names(
                        child_hash, child.names
                    )
                    dir_contents[hash_].add(child_hash)
            else:
                file_status[hash_] = make_names(hash_, item.names)

        self._fill_statuses(dir_status, local_exists, remote_exists)
        self._fill_statuses(file_status, local_exists, remote_exists)

        if log_missing:
            self._log_missing_caches(dict(dir_status, **file_status))

        return dir_status, file_status, dir_contents

    def _indexed_dir_hashes(self, cache, named_cache, dir_md5s):
        # Validate our index by verifying all indexed .dir hashes
        # still exist on the remote
        indexed_dirs = set(self.index.dir_hashes())
        indexed_dir_exists = set()
        if indexed_dirs:
            indexed_dir_exists.update(
                self.cache.list_hashes_exists(indexed_dirs)
            )
            missing_dirs = indexed_dirs.difference(indexed_dir_exists)
            if missing_dirs:
                logger.debug(
                    "Remote cache missing indexed .dir hashes '{}', "
                    "clearing remote index".format(", ".join(missing_dirs))
                )
                self.index.clear()

        # Check if non-indexed (new) dir hashes exist on remote
        dir_exists = dir_md5s.intersection(indexed_dir_exists)
        dir_exists.update(self.cache.list_hashes_exists(dir_md5s - dir_exists))

        # If .dir hash exists on the remote, assume directory contents
        # still exists on the remote
        for dir_hash in dir_exists:
            file_hashes = list(
                named_cache.child_keys(cache.tree.scheme, dir_hash)
            )
            if dir_hash not in self.index:
                logger.debug(
                    "Indexing new .dir '{}' with '{}' nested files".format(
                        dir_hash, len(file_hashes)
                    )
                )
                self.index.update([dir_hash], file_hashes)
            yield dir_hash
            yield from file_hashes

    @staticmethod
    def _fill_statuses(hash_info_dir, local_exists, remote_exists):
        # Using sets because they are way faster for lookups
        local = set(local_exists)
        remote = set(remote_exists)

        for md5, info in hash_info_dir.items():
            status = STATUS_MAP[(md5 in local, md5 in remote)]
            info["status"] = status

    def _get_plans(self, cache_obj, download, status_info, status):
        cache = []
        path_infos = []
        names = []
        hashes = []
        missing = []
        for md5, info in Tqdm(
            status_info.items(), desc="Analysing status", unit="file"
        ):
            if info["status"] == status:
                cache.append(cache_obj.tree.hash_to_path_info(md5))
                path_infos.append(self.tree.hash_to_path_info(md5))
                names.append(info["name"])
                hashes.append(md5)
            elif info["status"] == STATUS_MISSING:
                missing.append(md5)

        if download:
            to_infos = cache
            from_infos = path_infos
        else:
            to_infos = path_infos
            from_infos = cache

        return (from_infos, to_infos, names, hashes), missing

    def _process(
        self,
        cache,
        named_cache,
        jobs=None,
        show_checksums=False,
        download=False,
    ):
        logger.debug(
            "Preparing to {} '{}'".format(
                "download data from" if download else "upload data to",
                self.tree.path_info,
            )
        )

        if download:
            func = partial(
                _log_exceptions(self.tree.download, "download"),
                dir_mode=cache.tree.dir_mode,
                file_mode=cache.tree.file_mode,
            )
            status = STATUS_DELETED
            desc = "Downloading"
        else:
            func = _log_exceptions(self.tree.upload, "upload")
            status = STATUS_NEW
            desc = "Uploading"

        if jobs is None:
            jobs = self.tree.JOBS

        dir_status, file_status, dir_contents = self._status(
            cache,
            named_cache,
            jobs=jobs,
            show_checksums=show_checksums,
            download=download,
        )

        dir_plans, _ = self._get_plans(cache, download, dir_status, status)
        file_plans, missing_files = self._get_plans(
            cache, download, file_status, status
        )

        total = len(dir_plans[0]) + len(file_plans[0])
        if total == 0:
            return 0

        with Tqdm(total=total, unit="file", desc=desc) as pbar:
            func = pbar.wrap_fn(func)
            with ThreadPoolExecutor(max_workers=jobs) as executor:
                if download:
                    from_infos, to_infos, names, _ = (
                        d + f for d, f in zip(dir_plans, file_plans)
                    )
                    fails = sum(
                        executor.map(func, from_infos, to_infos, names)
                    )
                else:
                    # for uploads, push files first, and any .dir files last

                    file_futures = {}
                    for from_info, to_info, name, hash_ in zip(*file_plans):
                        file_futures[hash_] = executor.submit(
                            func, from_info, to_info, name
                        )
                    dir_futures = {}
                    for from_info, to_info, name, dir_hash in zip(*dir_plans):
                        # if for some reason a file contained in this dir is
                        # missing both locally and in the remote, we want to
                        # push whatever file content we have, but should not
                        # push .dir file
                        for file_hash in missing_files:
                            if file_hash in dir_contents[dir_hash]:
                                logger.debug(
                                    "directory '%s' contains missing files,"
                                    "skipping .dir file upload",
                                    name,
                                )
                                break
                        else:
                            wait_futures = {
                                future
                                for file_hash, future in file_futures.items()
                                if file_hash in dir_contents[dir_hash]
                            }
                            dir_futures[dir_hash] = executor.submit(
                                self._dir_upload,
                                func,
                                wait_futures,
                                from_info,
                                to_info,
                                name,
                            )
                    fails = sum(
                        future.result()
                        for future in concat(
                            file_futures.values(), dir_futures.values()
                        )
                    )

        if fails:
            if download:
                self.index.clear()
                raise DownloadError(fails)
            raise UploadError(fails)

        if not download:
            # index successfully pushed dirs
            for dir_hash, future in dir_futures.items():
                if future.result() == 0:
                    file_hashes = dir_contents[dir_hash]
                    logger.debug(
                        "Indexing pushed dir '{}' with "
                        "'{}' nested files".format(dir_hash, len(file_hashes))
                    )
                    self.index.update([dir_hash], file_hashes)

        return len(dir_plans[0]) + len(file_plans[0])

    @staticmethod
    def _dir_upload(func, futures, from_info, to_info, name):
        for future in as_completed(futures):
            if future.result():
                # do not upload this .dir file if any file in this
                # directory failed to upload
                logger.debug(
                    "failed to upload full contents of '{}', "
                    "aborting .dir file upload".format(name)
                )
                logger.error(f"failed to upload '{from_info}' to '{to_info}'")
                return 1
        return func(from_info, to_info, name)

    @index_locked
    def push(self, cache, named_cache, jobs=None, show_checksums=False):
        return self._process(
            cache,
            named_cache,
            jobs=jobs,
            show_checksums=show_checksums,
            download=False,
        )

    @index_locked
    def pull(self, cache, named_cache, jobs=None, show_checksums=False):
        ret = self._process(
            cache,
            named_cache,
            jobs=jobs,
            show_checksums=show_checksums,
            download=True,
        )

        if not self.tree.verify:
            with cache.tree.state:
                for checksum in named_cache.scheme_keys("local"):
                    cache_file = cache.tree.hash_to_path_info(checksum)
                    if cache.tree.exists(cache_file):
                        # We can safely save here, as existing corrupted files
                        # will be removed upon status, while files corrupted
                        # during download will not be moved from tmp_file
                        # (see `BaseTree.download()`)
                        hash_info = HashInfo(
                            cache.tree.PARAM_CHECKSUM, checksum
                        )
                        cache.tree.state.save(cache_file, hash_info)

        return ret

    @staticmethod
    def _log_missing_caches(hash_info_dict):
        missing_caches = [
            (md5, info)
            for md5, info in hash_info_dict.items()
            if info["status"] == STATUS_MISSING
        ]
        if missing_caches:
            missing_desc = "\n".join(
                "name: {}, md5: {}".format(info["name"], md5)
                for md5, info in missing_caches
            )
            msg = (
                "Some of the cache files do not exist neither locally "
                "nor on remote. Missing cache files:\n{}".format(missing_desc)
            )
            logger.warning(msg)
