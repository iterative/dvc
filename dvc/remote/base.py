import hashlib
import itertools
import json
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import copy
from functools import partial, wraps
from multiprocessing import cpu_count
from operator import itemgetter
from urllib.parse import urlparse

from shortuuid import uuid

import dvc.prompt as prompt
from dvc.exceptions import (
    CheckoutError,
    ConfirmRemoveError,
    DvcException,
    DvcIgnoreInCollectedDirError,
    RemoteCacheRequiredError,
)
from dvc.ignore import DvcIgnore
from dvc.path_info import PathInfo, URLInfo, WindowsPathInfo
from dvc.progress import Tqdm
from dvc.remote.index import RemoteIndex, RemoteIndexNoop
from dvc.remote.slow_link_detection import slow_link_guard
from dvc.state import StateNoop
from dvc.utils import tmp_fname
from dvc.utils.fs import makedirs, move
from dvc.utils.http import open_url

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


class RemoteCmdError(DvcException):
    def __init__(self, remote, cmd, ret, err):
        super().__init__(
            "{remote} command '{cmd}' finished with non-zero return code"
            " {ret}': {err}".format(remote=remote, cmd=cmd, ret=ret, err=err)
        )


class RemoteActionNotImplemented(DvcException):
    def __init__(self, action, scheme):
        m = f"{action} is not supported for {scheme} remotes"
        super().__init__(m)


class RemoteMissingDepsError(DvcException):
    pass


class DirCacheError(DvcException):
    def __init__(self, hash_):
        super().__init__(
            f"Failed to load dir cache for hash value: '{hash_}'."
        )


def index_locked(f):
    @wraps(f)
    def wrapper(obj, named_cache, remote, *args, **kwargs):
        if hasattr(remote, "index"):
            with remote.index:
                return f(obj, named_cache, remote, *args, **kwargs)
        return f(obj, named_cache, remote, *args, **kwargs)

    return wrapper


class BaseRemoteTree:
    scheme = "base"
    REQUIRES = {}
    PATH_CLS = URLInfo
    JOBS = 4 * cpu_count()

    PARAM_RELPATH = "relpath"
    CHECKSUM_DIR_SUFFIX = ".dir"
    HASH_JOBS = max(1, min(4, cpu_count() // 2))
    DEFAULT_VERIFY = False
    LIST_OBJECT_PAGE_SIZE = 1000
    TRAVERSE_WEIGHT_MULTIPLIER = 5
    TRAVERSE_PREFIX_LEN = 3
    TRAVERSE_THRESHOLD_SIZE = 500000
    CAN_TRAVERSE = True

    CACHE_MODE = None
    SHARED_MODE_MAP = {None: (None, None), "group": (None, None)}
    PARAM_CHECKSUM = None

    state = StateNoop()

    def __init__(self, repo, config):
        self.repo = repo
        self.config = config

        self._check_requires(config)

        shared = config.get("shared")
        self._file_mode, self._dir_mode = self.SHARED_MODE_MAP[shared]

        self.hash_jobs = (
            config.get("hash_jobs")
            or (self.repo and self.repo.config["core"].get("hash_jobs"))
            or self.HASH_JOBS
        )
        self.verify = config.get("verify", self.DEFAULT_VERIFY)
        self.path_info = None

    @classmethod
    def get_missing_deps(cls):
        import importlib

        missing = []
        for package, module in cls.REQUIRES.items():
            try:
                importlib.import_module(module)
            except ImportError:
                missing.append(package)

        return missing

    def _check_requires(self, config):
        missing = self.get_missing_deps()
        if not missing:
            return

        url = config.get("url", f"{self.scheme}://")
        msg = (
            "URL '{}' is supported but requires these missing "
            "dependencies: {}. If you have installed dvc using pip, "
            "choose one of these options to proceed: \n"
            "\n"
            "    1) Install specific missing dependencies:\n"
            "        pip install {}\n"
            "    2) Install dvc package that includes those missing "
            "dependencies: \n"
            "        pip install 'dvc[{}]'\n"
            "    3) Install dvc package with all possible "
            "dependencies included: \n"
            "        pip install 'dvc[all]'\n"
            "\n"
            "If you have installed dvc from a binary package and you "
            "are still seeing this message, please report it to us "
            "using https://github.com/iterative/dvc/issues. Thank you!"
        ).format(url, missing, " ".join(missing), self.scheme)
        raise RemoteMissingDepsError(msg)

    @classmethod
    def supported(cls, config):
        if isinstance(config, (str, bytes)):
            url = config
        else:
            url = config["url"]

        # NOTE: silently skipping remote, calling code should handle that
        parsed = urlparse(url)
        return parsed.scheme == cls.scheme

    @property
    def file_mode(self):
        return self._file_mode

    @property
    def dir_mode(self):
        return self._dir_mode

    @property
    def cache(self):
        return getattr(self.repo.cache, self.scheme)

    def open(self, path_info, mode="r", encoding=None):
        if hasattr(self, "_generate_download_url"):
            func = self._generate_download_url  # noqa,pylint:disable=no-member
            get_url = partial(func, path_info)
            return open_url(get_url, mode=mode, encoding=encoding)

        raise RemoteActionNotImplemented("open", self.scheme)

    def exists(self, path_info):
        raise NotImplementedError

    # pylint: disable=unused-argument

    def isdir(self, path_info):
        """Optional: Overwrite only if the remote has a way to distinguish
        between a directory and a file.
        """
        return False

    def isfile(self, path_info):
        """Optional: Overwrite only if the remote has a way to distinguish
        between a directory and a file.
        """
        return True

    def iscopy(self, path_info):
        """Check if this file is an independent copy."""
        return False  # We can't be sure by default

    def walk_files(self, path_info, **kwargs):
        """Return a generator with `PathInfo`s to all the files"""
        raise NotImplementedError

    def is_empty(self, path_info):
        return False

    def remove(self, path_info):
        raise RemoteActionNotImplemented("remove", self.scheme)

    def makedirs(self, path_info):
        """Optional: Implement only if the remote needs to create
        directories before copying/linking/moving data
        """

    def move(self, from_info, to_info, mode=None):
        assert mode is None
        self.copy(from_info, to_info)
        self.remove(from_info)

    def copy(self, from_info, to_info):
        raise RemoteActionNotImplemented("copy", self.scheme)

    def copy_fobj(self, fobj, to_info):
        raise RemoteActionNotImplemented("copy_fobj", self.scheme)

    def symlink(self, from_info, to_info):
        raise RemoteActionNotImplemented("symlink", self.scheme)

    def hardlink(self, from_info, to_info):
        raise RemoteActionNotImplemented("hardlink", self.scheme)

    def reflink(self, from_info, to_info):
        raise RemoteActionNotImplemented("reflink", self.scheme)

    @staticmethod
    def protect(path_info):
        pass

    def is_protected(self, path_info):
        return False

    # pylint: enable=unused-argument

    @staticmethod
    def unprotect(path_info):
        pass

    @classmethod
    def is_dir_hash(cls, hash_):
        if not hash_:
            return False
        return hash_.endswith(cls.CHECKSUM_DIR_SUFFIX)

    def get_hash(self, path_info, tree=None, **kwargs):
        assert isinstance(path_info, str) or path_info.scheme == self.scheme

        if not tree:
            tree = self

        if not tree.exists(path_info):
            return None

        if tree == self:
            # pylint: disable=assignment-from-none
            hash_ = self.state.get(path_info)
        else:
            hash_ = None
        # If we have dir hash in state db, but dir cache file is lost,
        # then we need to recollect the dir via .get_dir_hash() call below,
        # see https://github.com/iterative/dvc/issues/2219 for context
        if (
            hash_
            and self.is_dir_hash(hash_)
            and not tree.exists(self.cache.hash_to_path_info(hash_))
        ):
            hash_ = None

        if hash_:
            return hash_

        if tree.isdir(path_info):
            hash_ = self.get_dir_hash(path_info, tree, **kwargs)
        else:
            hash_ = tree.get_file_hash(path_info)

        if hash_ and self.exists(path_info):
            self.state.save(path_info, hash_)

        return hash_

    def get_file_hash(self, path_info):
        raise NotImplementedError

    def get_dir_hash(self, path_info, tree, **kwargs):
        if not self.cache:
            raise RemoteCacheRequiredError(path_info)

        dir_info = self._collect_dir(path_info, tree, **kwargs)
        return self._save_dir_info(dir_info, path_info)

    def hash_to_path_info(self, hash_):
        return self.path_info / hash_[0:2] / hash_[2:]

    def path_to_hash(self, path):
        parts = self.PATH_CLS(path).parts[-2:]

        if not (len(parts) == 2 and parts[0] and len(parts[0]) == 2):
            raise ValueError(f"Bad cache file path '{path}'")

        return "".join(parts)

    def save_info(self, path_info, tree=None, **kwargs):
        return {
            self.PARAM_CHECKSUM: self.get_hash(path_info, tree=tree, **kwargs)
        }

    @staticmethod
    def _calculate_hashes(file_infos, tree):
        file_infos = list(file_infos)
        with Tqdm(
            total=len(file_infos),
            unit="md5",
            desc="Computing file/dir hashes (only done once)",
        ) as pbar:
            worker = pbar.wrap_fn(tree.get_file_hash)
            with ThreadPoolExecutor(max_workers=tree.hash_jobs) as executor:
                tasks = executor.map(worker, file_infos)
                hashes = dict(zip(file_infos, tasks))
        return hashes

    def _collect_dir(self, path_info, tree, **kwargs):
        file_infos = set()

        for fname in tree.walk_files(path_info, **kwargs):
            if DvcIgnore.DVCIGNORE_FILE == fname.name:
                raise DvcIgnoreInCollectedDirError(fname.parent)

            file_infos.add(fname)

        hashes = {fi: self.state.get(fi) for fi in file_infos}
        not_in_state = {fi for fi, hash_ in hashes.items() if hash_ is None}

        new_hashes = self._calculate_hashes(not_in_state, tree)
        hashes.update(new_hashes)

        result = [
            {
                self.PARAM_CHECKSUM: hashes[fi],
                # NOTE: this is lossy transformation:
                #   "hey\there" -> "hey/there"
                #   "hey/there" -> "hey/there"
                # The latter is fine filename on Windows, which
                # will transform to dir/file on back transform.
                #
                # Yes, this is a BUG, as long as we permit "/" in
                # filenames on Windows and "\" on Unix
                self.PARAM_RELPATH: fi.relative_to(path_info).as_posix(),
            }
            for fi in file_infos
        ]

        # Sorting the list by path to ensure reproducibility
        return sorted(result, key=itemgetter(self.PARAM_RELPATH))

    def _save_dir_info(self, dir_info, path_info):
        hash_, tmp_info = self._get_dir_info_hash(dir_info)
        new_info = self.cache.hash_to_path_info(hash_)
        if self.cache.changed_cache_file(hash_):
            self.cache.tree.makedirs(new_info.parent)
            self.cache.tree.move(
                tmp_info, new_info, mode=self.cache.CACHE_MODE
            )

        if self.exists(path_info):
            self.state.save(path_info, hash_)
        self.state.save(new_info, hash_)

        return hash_

    def _get_dir_info_hash(self, dir_info):
        tmp = tempfile.NamedTemporaryFile(delete=False).name
        with open(tmp, "w+") as fobj:
            json.dump(dir_info, fobj, sort_keys=True)

        tree = self.cache.tree
        from_info = PathInfo(tmp)
        to_info = tree.path_info / tmp_fname("")
        tree.upload(from_info, to_info, no_progress_bar=True)

        hash_ = tree.get_file_hash(to_info) + self.CHECKSUM_DIR_SUFFIX
        return hash_, to_info

    def upload(self, from_info, to_info, name=None, no_progress_bar=False):
        if not hasattr(self, "_upload"):
            raise RemoteActionNotImplemented("upload", self.scheme)

        if to_info.scheme != self.scheme:
            raise NotImplementedError

        if from_info.scheme != "local":
            raise NotImplementedError

        logger.debug("Uploading '%s' to '%s'", from_info, to_info)

        name = name or from_info.name

        self._upload(  # noqa, pylint: disable=no-member
            from_info.fspath,
            to_info,
            name=name,
            no_progress_bar=no_progress_bar,
        )

    def download(
        self,
        from_info,
        to_info,
        name=None,
        no_progress_bar=False,
        file_mode=None,
        dir_mode=None,
    ):
        if not hasattr(self, "_download"):
            raise RemoteActionNotImplemented("download", self.scheme)

        if from_info.scheme != self.scheme:
            raise NotImplementedError

        if to_info.scheme == self.scheme != "local":
            self.copy(from_info, to_info)
            return 0

        if to_info.scheme != "local":
            raise NotImplementedError

        if self.isdir(from_info):
            return self._download_dir(
                from_info, to_info, name, no_progress_bar, file_mode, dir_mode
            )
        return self._download_file(
            from_info, to_info, name, no_progress_bar, file_mode, dir_mode
        )

    def _download_dir(
        self, from_info, to_info, name, no_progress_bar, file_mode, dir_mode
    ):
        from_infos = list(self.walk_files(from_info))
        to_infos = (
            to_info / info.relative_to(from_info) for info in from_infos
        )

        with Tqdm(
            total=len(from_infos),
            desc="Downloading directory",
            unit="Files",
            disable=no_progress_bar,
        ) as pbar:
            download_files = pbar.wrap_fn(
                partial(
                    self._download_file,
                    name=name,
                    no_progress_bar=True,
                    file_mode=file_mode,
                    dir_mode=dir_mode,
                )
            )
            with ThreadPoolExecutor(max_workers=self.JOBS) as executor:
                futures = [
                    executor.submit(download_files, from_info, to_info)
                    for from_info, to_info in zip(from_infos, to_infos)
                ]

                # NOTE: unlike pulling/fetching cache, where we need to
                # download everything we can, not raising an error here might
                # turn very ugly, as the user might think that he has
                # downloaded a complete directory, while having a partial one,
                # which might cause unexpected results in his pipeline.
                for future in as_completed(futures):
                    # NOTE: executor won't let us raise until all futures that
                    # it has are finished, so we need to cancel them ourselves
                    # before re-raising.
                    exc = future.exception()
                    if exc:
                        for entry in futures:
                            entry.cancel()
                        raise exc

    def _download_file(
        self, from_info, to_info, name, no_progress_bar, file_mode, dir_mode
    ):
        makedirs(to_info.parent, exist_ok=True, mode=dir_mode)

        logger.debug("Downloading '%s' to '%s'", from_info, to_info)
        name = name or to_info.name

        tmp_file = tmp_fname(to_info)

        self._download(  # noqa, pylint: disable=no-member
            from_info, tmp_file, name=name, no_progress_bar=no_progress_bar
        )

        move(tmp_file, to_info, mode=file_mode)

    def list_paths(self, prefix=None, progress_callback=None):
        if prefix:
            if len(prefix) > 2:
                path_info = self.path_info / prefix[:2] / prefix[2:]
            else:
                path_info = self.path_info / prefix[:2]
        else:
            path_info = self.path_info
        if progress_callback:
            for file_info in self.walk_files(path_info):
                progress_callback()
                yield file_info.path
        else:
            yield from self.walk_files(path_info)

    def list_hashes(self, prefix=None, progress_callback=None):
        """Iterate over hashes in this tree.

        If `prefix` is specified, only hashes which begin with `prefix`
        will be returned.
        """
        for path in self.list_paths(prefix, progress_callback):
            try:
                yield self.path_to_hash(path)
            except ValueError:
                logger.debug(
                    "'%s' doesn't look like a cache file, skipping", path
                )

    def all(self, jobs=None, name=None):
        """Iterate over all hashes in this tree.

        Hashes will be fetched in parallel threads according to prefix
        (except for small remotes) and a progress bar will be displayed.
        """
        logger.debug(
            "Fetching all hashes from '{}'".format(
                name if name else "remote cache"
            )
        )

        if not self.CAN_TRAVERSE:
            return self.list_hashes()

        remote_size, remote_hashes = self.estimate_remote_size(name=name)
        return self.list_hashes_traverse(
            remote_size, remote_hashes, jobs, name
        )

    def _hashes_with_limit(self, limit, prefix=None, progress_callback=None):
        count = 0
        for hash_ in self.list_hashes(prefix, progress_callback):
            yield hash_
            count += 1
            if count > limit:
                logger.debug(
                    "`list_hashes()` returned max '{}' hashes, "
                    "skipping remaining results".format(limit)
                )
                return

    def _max_estimation_size(self, hashes):
        # Max remote size allowed for us to use traverse method
        return max(
            self.TRAVERSE_THRESHOLD_SIZE,
            len(hashes)
            / self.TRAVERSE_WEIGHT_MULTIPLIER
            * self.LIST_OBJECT_PAGE_SIZE,
        )

    def estimate_remote_size(self, hashes=None, name=None):
        """Estimate tree size based on number of entries beginning with
        "00..." prefix.
        """
        prefix = "0" * self.TRAVERSE_PREFIX_LEN
        total_prefixes = pow(16, self.TRAVERSE_PREFIX_LEN)
        if hashes:
            max_hashes = self._max_estimation_size(hashes)
        else:
            max_hashes = None

        with Tqdm(
            desc="Estimating size of "
            + (f"cache in '{name}'" if name else "remote cache"),
            unit="file",
        ) as pbar:

            def update(n=1):
                pbar.update(n * total_prefixes)

            if max_hashes:
                hashes = self._hashes_with_limit(
                    max_hashes / total_prefixes, prefix, update
                )
            else:
                hashes = self.list_hashes(prefix, update)

            remote_hashes = set(hashes)
            if remote_hashes:
                remote_size = total_prefixes * len(remote_hashes)
            else:
                remote_size = total_prefixes
            logger.debug(f"Estimated remote size: {remote_size} files")
        return remote_size, remote_hashes

    def list_hashes_traverse(
        self, remote_size, remote_hashes, jobs=None, name=None
    ):
        """Iterate over all hashes found in this tree.
        Hashes are fetched in parallel according to prefix, except in
        cases where the remote size is very small.

        All hashes from the remote (including any from the size
        estimation step passed via the `remote_hashes` argument) will be
        returned.

        NOTE: For large remotes the list of hashes will be very
        big(e.g. 100M entries, md5 for each is 32 bytes, so ~3200Mb list)
        and we don't really need all of it at the same time, so it makes
        sense to use a generator to gradually iterate over it, without
        keeping all of it in memory.
        """
        num_pages = remote_size / self.LIST_OBJECT_PAGE_SIZE
        if num_pages < 256 / self.JOBS:
            # Fetching prefixes in parallel requires at least 255 more
            # requests, for small enough remotes it will be faster to fetch
            # entire cache without splitting it into prefixes.
            #
            # NOTE: this ends up re-fetching hashes that were already
            # fetched during remote size estimation
            traverse_prefixes = [None]
            initial = 0
        else:
            yield from remote_hashes
            initial = len(remote_hashes)
            traverse_prefixes = [f"{i:02x}" for i in range(1, 256)]
            if self.TRAVERSE_PREFIX_LEN > 2:
                traverse_prefixes += [
                    "{0:0{1}x}".format(i, self.TRAVERSE_PREFIX_LEN)
                    for i in range(1, pow(16, self.TRAVERSE_PREFIX_LEN - 2))
                ]
        with Tqdm(
            desc="Querying "
            + (f"cache in '{name}'" if name else "remote cache"),
            total=remote_size,
            initial=initial,
            unit="file",
        ) as pbar:

            def list_with_update(prefix):
                return list(
                    self.list_hashes(
                        prefix=prefix, progress_callback=pbar.update
                    )
                )

            with ThreadPoolExecutor(max_workers=jobs or self.JOBS) as executor:
                in_remote = executor.map(list_with_update, traverse_prefixes,)
                yield from itertools.chain.from_iterable(in_remote)

    def list_hashes_exists(self, hashes, jobs=None, name=None):
        """Return list of the specified hashes which exist in this tree.
        Hashes will be queried individually.
        """
        logger.debug(
            "Querying {} hashes via object_exists".format(len(hashes))
        )
        with Tqdm(
            desc="Querying "
            + ("cache in " + name if name else "remote cache"),
            total=len(hashes),
            unit="file",
        ) as pbar:

            def exists_with_progress(path_info):
                ret = self.exists(path_info)
                pbar.update_msg(str(path_info))
                return ret

            with ThreadPoolExecutor(max_workers=jobs or self.JOBS) as executor:
                path_infos = map(self.hash_to_path_info, hashes)
                in_remote = executor.map(exists_with_progress, path_infos)
                ret = list(itertools.compress(hashes, in_remote))
                return ret

    def _remove_unpacked_dir(self, hash_):
        pass


class Remote:
    """Cloud remote class.

    Provides methods for indexing and garbage collecting trees which contain
    DVC remotes.
    """

    INDEX_CLS = RemoteIndex

    def __init__(self, tree):
        self.tree = tree
        self.repo = tree.repo

        config = tree.config
        url = config.get("url")
        if url:
            index_name = hashlib.sha256(url.encode("utf-8")).hexdigest()
            self.index = self.INDEX_CLS(
                self.repo, index_name, dir_suffix=self.tree.CHECKSUM_DIR_SUFFIX
            )
        else:
            self.index = RemoteIndexNoop()

    @property
    def path_info(self):
        return self.tree.path_info

    def __repr__(self):
        return "{class_name}: '{path_info}'".format(
            class_name=type(self).__name__,
            path_info=self.path_info or "No path",
        )

    @property
    def cache(self):
        return getattr(self.repo.cache, self.scheme)

    @property
    def scheme(self):
        return self.tree.scheme

    def is_dir_hash(self, hash_):
        return self.tree.is_dir_hash(hash_)

    def get_hash(self, path_info, **kwargs):
        return self.tree.get_hash(path_info, **kwargs)

    def hash_to_path_info(self, hash_):
        return self.tree.hash_to_path_info(hash_)

    def path_to_hash(self, path):
        return self.tree.path_to_hash(path)

    def save_info(self, path_info, **kwargs):
        return self.tree.save_info(path_info, **kwargs)

    def open(self, *args, **kwargs):
        return self.tree.open(*args, **kwargs)

    def hashes_exist(self, hashes, jobs=None, name=None):
        """Check if the given hashes are stored in the remote.

        There are two ways of performing this check:

        - Traverse method: Get a list of all the files in the remote
            (traversing the cache directory) and compare it with
            the given hashes. Cache entries will be retrieved in parallel
            threads according to prefix (i.e. entries starting with, "00...",
            "01...", and so on) and a progress bar will be displayed.

        - Exists method: For each given hash, run the `exists`
            method and filter the hashes that aren't on the remote.
            This is done in parallel threads.
            It also shows a progress bar when performing the check.

        The reason for such an odd logic is that most of the remotes
        take much shorter time to just retrieve everything they have under
        a certain prefix (e.g. s3, gs, ssh, hdfs). Other remotes that can
        check if particular file exists much quicker, use their own
        implementation of hashes_exist (see ssh, local).

        Which method to use will be automatically determined after estimating
        the size of the remote cache, and comparing the estimated size with
        len(hashes). To estimate the size of the remote cache, we fetch
        a small subset of cache entries (i.e. entries starting with "00...").
        Based on the number of entries in that subset, the size of the full
        cache can be estimated, since the cache is evenly distributed according
        to hash.

        Returns:
            A list with hashes that were found in the remote
        """
        # Remotes which do not use traverse prefix should override
        # hashes_exist() (see ssh, local)
        assert self.tree.TRAVERSE_PREFIX_LEN >= 2

        hashes = set(hashes)
        indexed_hashes = set(self.index.intersection(hashes))
        hashes -= indexed_hashes
        logger.debug("Matched '{}' indexed hashes".format(len(indexed_hashes)))
        if not hashes:
            return indexed_hashes

        if len(hashes) == 1 or not self.tree.CAN_TRAVERSE:
            remote_hashes = self.tree.list_hashes_exists(hashes, jobs, name)
            return list(indexed_hashes) + remote_hashes

        # Max remote size allowed for us to use traverse method
        remote_size, remote_hashes = self.tree.estimate_remote_size(
            hashes, name
        )

        traverse_pages = remote_size / self.tree.LIST_OBJECT_PAGE_SIZE
        # For sufficiently large remotes, traverse must be weighted to account
        # for performance overhead from large lists/sets.
        # From testing with S3, for remotes with 1M+ files, object_exists is
        # faster until len(hashes) is at least 10k~100k
        if remote_size > self.tree.TRAVERSE_THRESHOLD_SIZE:
            traverse_weight = (
                traverse_pages * self.tree.TRAVERSE_WEIGHT_MULTIPLIER
            )
        else:
            traverse_weight = traverse_pages
        if len(hashes) < traverse_weight:
            logger.debug(
                "Large remote ('{}' hashes < '{}' traverse weight), "
                "using object_exists for remaining hashes".format(
                    len(hashes), traverse_weight
                )
            )
            return (
                list(indexed_hashes)
                + list(hashes & remote_hashes)
                + self.tree.list_hashes_exists(
                    hashes - remote_hashes, jobs, name
                )
            )

        logger.debug("Querying '{}' hashes via traverse".format(len(hashes)))
        remote_hashes = set(
            self.tree.list_hashes_traverse(
                remote_size, remote_hashes, jobs, name
            )
        )
        return list(indexed_hashes) + list(hashes & set(remote_hashes))

    @classmethod
    @index_locked
    def gc(cls, named_cache, remote, jobs=None):
        tree = remote.tree
        used = set(named_cache.scheme_keys("local"))

        if tree.scheme != "":
            used.update(named_cache.scheme_keys(tree.scheme))

        removed = False
        # hashes must be sorted to ensure we always remove .dir files first
        for hash_ in sorted(
            tree.all(jobs, str(tree.path_info)),
            key=tree.is_dir_hash,
            reverse=True,
        ):
            if hash_ in used:
                continue
            path_info = tree.hash_to_path_info(hash_)
            if tree.is_dir_hash(hash_):
                # backward compatibility
                # pylint: disable=protected-access
                tree._remove_unpacked_dir(hash_)
            tree.remove(path_info)
            removed = True

        if removed and hasattr(remote, "index"):
            remote.index.clear()
        return removed


class CloudCache:
    """Cloud cache class."""

    DEFAULT_CACHE_TYPES = ["copy"]
    CACHE_MODE = BaseRemoteTree.CACHE_MODE

    def __init__(self, tree):
        self.tree = tree
        self.repo = tree.repo

        self.cache_types = tree.config.get("type") or copy(
            self.DEFAULT_CACHE_TYPES
        )
        self.cache_type_confirmed = False
        self._dir_info = {}

    @property
    def path_info(self):
        return self.tree.path_info

    @property
    def cache(self):
        return getattr(self.repo.cache, self.scheme)

    @property
    def scheme(self):
        return self.tree.scheme

    @property
    def state(self):
        return self.tree.state

    def open(self, *args, **kwargs):
        return self.tree.open(*args, **kwargs)

    def is_dir_hash(self, hash_):
        return self.tree.is_dir_hash(hash_)

    def get_hash(self, path_info, **kwargs):
        return self.tree.get_hash(path_info, **kwargs)

    # Override to return path as a string instead of PathInfo for clouds
    # which support string paths (see local)
    def hash_to_path(self, hash_):
        return self.hash_to_path_info(hash_)

    def hash_to_path_info(self, hash_):
        return self.tree.hash_to_path_info(hash_)

    def get_dir_cache(self, hash_):
        assert hash_

        dir_info = self._dir_info.get(hash_)
        if dir_info:
            return dir_info

        try:
            dir_info = self.load_dir_cache(hash_)
        except DirCacheError:
            dir_info = []

        self._dir_info[hash_] = dir_info
        return dir_info

    def load_dir_cache(self, hash_):
        path_info = self.hash_to_path_info(hash_)

        try:
            with self.cache.open(path_info, "r") as fobj:
                d = json.load(fobj)
        except (ValueError, FileNotFoundError) as exc:
            raise DirCacheError(hash_) from exc

        if not isinstance(d, list):
            logger.error(
                "dir cache file format error '%s' [skipping the file]",
                path_info,
            )
            return []

        if self.tree.PATH_CLS == WindowsPathInfo:
            # only need to convert it for Windows
            for info in d:
                # NOTE: here is a BUG, see comment to .as_posix() below
                relpath = info[self.tree.PARAM_RELPATH]
                info[self.tree.PARAM_RELPATH] = relpath.replace(
                    "/", self.tree.PATH_CLS.sep
                )

        return d

    def changed(self, path_info, hash_info):
        """Checks if data has changed.

        A file is considered changed if:
            - It doesn't exist on the working directory (was unlinked)
            - Hash value is not computed (saving a new file)
            - The hash value stored is different from the given one
            - There's no file in the cache

        Args:
            path_info: dict with path information.
            hash: expected hash value for this data.

        Returns:
            bool: True if data has changed, False otherwise.
        """

        logger.debug(
            "checking if '%s'('%s') has changed.", path_info, hash_info
        )

        if not self.tree.exists(path_info):
            logger.debug("'%s' doesn't exist.", path_info)
            return True

        hash_ = hash_info.get(self.tree.PARAM_CHECKSUM)
        if hash_ is None:
            logger.debug("hash value for '%s' is missing.", path_info)
            return True

        if self.changed_cache(hash_):
            logger.debug("cache for '%s'('%s') has changed.", path_info, hash_)
            return True

        actual = self.get_hash(path_info)
        if hash_ != actual:
            logger.debug(
                "hash value '%s' for '%s' has changed (actual '%s').",
                hash_,
                actual,
                path_info,
            )
            return True

        logger.debug("'%s' hasn't changed.", path_info)
        return False

    def link(self, from_info, to_info):
        self._link(from_info, to_info, self.cache_types)

    def _link(self, from_info, to_info, link_types):
        assert self.tree.isfile(from_info)

        self.tree.makedirs(to_info.parent)

        self._try_links(from_info, to_info, link_types)

    def _verify_link(self, path_info, link_type):
        if self.cache_type_confirmed:
            return

        is_link = getattr(self.tree, f"is_{link_type}", None)
        if is_link and not is_link(path_info):
            self.tree.remove(path_info)
            raise DvcException(f"failed to verify {link_type}")

        self.cache_type_confirmed = True

    @slow_link_guard
    def _try_links(self, from_info, to_info, link_types):
        while link_types:
            link_method = getattr(self.tree, link_types[0])
            try:
                self._do_link(from_info, to_info, link_method)
                self._verify_link(to_info, link_types[0])
                return

            except DvcException as exc:
                logger.debug(
                    "Cache type '%s' is not supported: %s", link_types[0], exc
                )
                del link_types[0]

        raise DvcException("no possible cache types left to try out.")

    def _do_link(self, from_info, to_info, link_method):
        if self.tree.exists(to_info):
            raise DvcException(f"Link '{to_info}' already exists!")

        link_method(from_info, to_info)

        logger.debug(
            "Created '%s': %s -> %s", self.cache_types[0], from_info, to_info,
        )

    def _save_file(self, path_info, tree, hash_, save_link=True, **kwargs):
        assert hash_

        cache_info = self.hash_to_path_info(hash_)
        if tree == self.tree:
            if self.changed_cache(hash_):
                self.tree.move(path_info, cache_info, mode=self.CACHE_MODE)
                self.link(cache_info, path_info)
            elif self.tree.iscopy(path_info) and self._cache_is_copy(
                path_info
            ):
                # Default relink procedure involves unneeded copy
                self.tree.unprotect(path_info)
            else:
                self.tree.remove(path_info)
                self.link(cache_info, path_info)

            if save_link:
                self.state.save_link(path_info)
            # we need to update path and cache, since in case of reflink,
            # or copy cache type moving original file results in updates on
            # next executed command, which causes md5 recalculation
            self.state.save(path_info, hash_)
        else:
            if self.changed_cache(hash_):
                with tree.open(path_info, mode="rb") as fobj:
                    # if tree has fetch enabled, DVC out will be fetched on
                    # open and we do not need to read/copy any data
                    if not (
                        tree.isdvc(path_info, strict=False) and tree.fetch
                    ):
                        self.tree.copy_fobj(fobj, cache_info)
                callback = kwargs.get("download_callback")
                if callback:
                    callback(1)

        self.state.save(cache_info, hash_)
        return {self.tree.PARAM_CHECKSUM: hash_}

    def _cache_is_copy(self, path_info):
        """Checks whether cache uses copies."""
        if self.cache_type_confirmed:
            return self.cache_types[0] == "copy"

        if set(self.cache_types) <= {"copy"}:
            return True

        workspace_file = path_info.with_name("." + uuid())
        test_cache_file = self.path_info / ".cache_type_test_file"
        if not self.tree.exists(test_cache_file):
            with self.tree.open(test_cache_file, "wb") as fobj:
                fobj.write(bytes(1))
        try:
            self.link(test_cache_file, workspace_file)
        finally:
            self.tree.remove(workspace_file)
            self.tree.remove(test_cache_file)

        self.cache_type_confirmed = True
        return self.cache_types[0] == "copy"

    def _save_dir(self, path_info, tree, hash_, save_link=True, **kwargs):
        dir_info = self.get_dir_cache(hash_)
        for entry in Tqdm(
            dir_info, desc="Saving " + path_info.name, unit="file"
        ):
            entry_info = path_info / entry[self.tree.PARAM_RELPATH]
            entry_hash = entry[self.tree.PARAM_CHECKSUM]
            self._save_file(
                entry_info, tree, entry_hash, save_link=False, **kwargs
            )

        if save_link:
            self.state.save_link(path_info)
        if self.tree.exists(path_info):
            self.state.save(path_info, hash_)

        cache_info = self.hash_to_path_info(hash_)
        self.state.save(cache_info, hash_)
        return {self.tree.PARAM_CHECKSUM: hash_}

    def save(self, path_info, tree, hash_info, save_link=True, **kwargs):
        if path_info.scheme != self.scheme:
            raise RemoteActionNotImplemented(
                f"save {path_info.scheme} -> {self.scheme}", self.scheme,
            )

        if not hash_info:
            hash_info = self.tree.save_info(path_info, tree=tree, **kwargs)
        hash_ = hash_info[self.tree.PARAM_CHECKSUM]
        return self._save(path_info, tree, hash_, save_link, **kwargs)

    def _save(self, path_info, tree, hash_, save_link=True, **kwargs):
        to_info = self.hash_to_path_info(hash_)
        logger.debug("Saving '%s' to '%s'.", path_info, to_info)

        if tree.isdir(path_info):
            return self._save_dir(path_info, tree, hash_, save_link, **kwargs)
        return self._save_file(path_info, tree, hash_, save_link, **kwargs)

    def changed_cache_file(self, hash_):
        """Compare the given hash with the (corresponding) actual one.

        - Use `State` as a cache for computed hashes
            + The entries are invalidated by taking into account the following:
                * mtime
                * inode
                * size
                * hash

        - Remove the file from cache if it doesn't match the actual hash
        """
        # Prefer string path over PathInfo when possible due to performance
        cache_info = self.hash_to_path(hash_)
        if self.tree.is_protected(cache_info):
            logger.debug(
                "Assuming '%s' is unchanged since it is read-only", cache_info
            )
            return False

        actual = self.get_hash(cache_info)

        logger.debug(
            "cache '%s' expected '%s' actual '%s'", cache_info, hash_, actual,
        )

        if not hash_ or not actual:
            return True

        if actual.split(".")[0] == hash_.split(".")[0]:
            # making cache file read-only so we don't need to check it
            # next time
            self.tree.protect(cache_info)
            return False

        if self.tree.exists(cache_info):
            logger.warning("corrupted cache file '%s'.", cache_info)
            self.tree.remove(cache_info)

        return True

    def _changed_dir_cache(self, hash_, path_info=None, filter_info=None):
        if self.changed_cache_file(hash_):
            return True

        for entry in self.get_dir_cache(hash_):
            entry_hash = entry[self.tree.PARAM_CHECKSUM]

            if path_info and filter_info:
                entry_info = path_info / entry[self.tree.PARAM_RELPATH]
                if not entry_info.isin_or_eq(filter_info):
                    continue

            if self.changed_cache_file(entry_hash):
                return True

        return False

    def changed_cache(self, hash_, path_info=None, filter_info=None):
        if self.is_dir_hash(hash_):
            return self._changed_dir_cache(
                hash_, path_info=path_info, filter_info=filter_info
            )
        return self.changed_cache_file(hash_)

    def already_cached(self, path_info):
        current = self.get_hash(path_info)

        if not current:
            return False

        return not self.changed_cache(current)

    def safe_remove(self, path_info, force=False):
        if not self.tree.exists(path_info):
            return

        if not force and not self.already_cached(path_info):
            msg = (
                "file '{}' is going to be removed."
                " Are you sure you want to proceed?".format(str(path_info))
            )

            if not prompt.confirm(msg):
                raise ConfirmRemoveError(str(path_info))

        self.tree.remove(path_info)

    def _checkout_file(
        self, path_info, hash_, force, progress_callback=None, relink=False
    ):
        """The file is changed we need to checkout a new copy"""
        added, modified = True, False
        cache_info = self.hash_to_path_info(hash_)
        if self.tree.exists(path_info):
            logger.debug("data '%s' will be replaced.", path_info)
            self.safe_remove(path_info, force=force)
            added, modified = False, True

        self.link(cache_info, path_info)
        self.state.save_link(path_info)
        self.state.save(path_info, hash_)
        if progress_callback:
            progress_callback(str(path_info))

        return added, modified and not relink

    def _checkout_dir(
        self,
        path_info,
        hash_,
        force,
        progress_callback=None,
        relink=False,
        filter_info=None,
    ):
        added, modified = False, False
        # Create dir separately so that dir is created
        # even if there are no files in it
        if not self.tree.exists(path_info):
            added = True
            self.tree.makedirs(path_info)

        dir_info = self.get_dir_cache(hash_)

        logger.debug("Linking directory '%s'.", path_info)

        for entry in dir_info:
            relative_path = entry[self.tree.PARAM_RELPATH]
            entry_hash = entry[self.tree.PARAM_CHECKSUM]
            entry_cache_info = self.hash_to_path_info(entry_hash)
            entry_info = path_info / relative_path

            if filter_info and not entry_info.isin_or_eq(filter_info):
                continue

            entry_hash_info = {self.tree.PARAM_CHECKSUM: entry_hash}
            if relink or self.changed(entry_info, entry_hash_info):
                modified = True
                self.safe_remove(entry_info, force=force)
                self.link(entry_cache_info, entry_info)
                self.state.save(entry_info, entry_hash)
            if progress_callback:
                progress_callback(str(entry_info))

        modified = (
            self._remove_redundant_files(path_info, dir_info, force)
            or modified
        )

        self.state.save_link(path_info)
        self.state.save(path_info, hash_)

        # relink is not modified, assume it as nochange
        return added, not added and modified and not relink

    def _remove_redundant_files(self, path_info, dir_info, force):
        existing_files = set(self.tree.walk_files(path_info))

        needed_files = {
            path_info / entry[self.tree.PARAM_RELPATH] for entry in dir_info
        }
        redundant_files = existing_files - needed_files
        for path in redundant_files:
            self.safe_remove(path, force)

        return bool(redundant_files)

    def checkout(
        self,
        path_info,
        hash_info,
        force=False,
        progress_callback=None,
        relink=False,
        filter_info=None,
    ):
        if path_info.scheme not in ["local", self.scheme]:
            raise NotImplementedError

        hash_ = hash_info.get(self.tree.PARAM_CHECKSUM)
        failed = None
        skip = False
        if not hash_:
            logger.warning(
                "No file hash info found for '%s'. " "It won't be created.",
                path_info,
            )
            self.safe_remove(path_info, force=force)
            failed = path_info

        elif not relink and not self.changed(path_info, hash_info):
            logger.debug("Data '%s' didn't change.", path_info)
            skip = True

        elif self.changed_cache(
            hash_, path_info=path_info, filter_info=filter_info
        ):
            logger.warning(
                "Cache '%s' not found. File '%s' won't be created.",
                hash_,
                path_info,
            )
            self.safe_remove(path_info, force=force)
            failed = path_info

        if failed or skip:
            if progress_callback:
                progress_callback(
                    str(path_info),
                    self.get_files_number(self.path_info, hash_, filter_info),
                )
            if failed:
                raise CheckoutError([failed])
            return

        logger.debug("Checking out '%s' with cache '%s'.", path_info, hash_)

        return self._checkout(
            path_info, hash_, force, progress_callback, relink, filter_info,
        )

    def _checkout(
        self,
        path_info,
        hash_,
        force=False,
        progress_callback=None,
        relink=False,
        filter_info=None,
    ):
        if not self.is_dir_hash(hash_):
            return self._checkout_file(
                path_info, hash_, force, progress_callback, relink
            )

        return self._checkout_dir(
            path_info, hash_, force, progress_callback, relink, filter_info
        )

    def get_files_number(self, path_info, hash_, filter_info):
        from funcy.py3 import ilen

        if not hash_:
            return 0

        if not self.is_dir_hash(hash_):
            return 1

        if not filter_info:
            return len(self.get_dir_cache(hash_))

        return ilen(
            filter_info.isin_or_eq(path_info / entry[self.tree.PARAM_CHECKSUM])
            for entry in self.get_dir_cache(hash_)
        )
