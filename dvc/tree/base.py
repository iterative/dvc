import itertools
import json
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from multiprocessing import cpu_count
from operator import itemgetter
from urllib.parse import urlparse

from funcy import cached_property

from dvc.exceptions import (
    DvcException,
    DvcIgnoreInCollectedDirError,
    RemoteCacheRequiredError,
)
from dvc.ignore import DvcIgnore
from dvc.path_info import PathInfo, URLInfo
from dvc.progress import Tqdm
from dvc.state import StateNoop
from dvc.utils import tmp_fname
from dvc.utils.fs import makedirs, move
from dvc.utils.http import open_url

logger = logging.getLogger(__name__)


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


class BaseTree:
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

        self.verify = config.get("verify", self.DEFAULT_VERIFY)
        self.path_info = None

    @cached_property
    def hash_jobs(self):
        return (
            self.config.get("hash_jobs")
            or (self.repo and self.repo.config["core"].get("hash_jobs"))
            or self.HASH_JOBS
        )

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

    def exists(self, path_info, use_dvcignore=True):
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
        """Return a generator with `PathInfo`s to all the files.

        Optional kwargs:
            prefix (bool): If true `path_info` will be treated as a prefix
                rather than directory path.
        """
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

    def get_hash(self, path_info, **kwargs):
        assert path_info and (
            isinstance(path_info, str) or path_info.scheme == self.scheme
        )

        if not self.exists(path_info):
            return self.PARAM_CHECKSUM, None

        # pylint: disable=assignment-from-none
        hash_ = self.state.get(path_info)

        # If we have dir hash in state db, but dir cache file is lost,
        # then we need to recollect the dir via .get_dir_hash() call below,
        # see https://github.com/iterative/dvc/issues/2219 for context
        if (
            hash_
            and self.is_dir_hash(hash_)
            and not self.cache.tree.exists(
                self.cache.tree.hash_to_path_info(hash_)
            )
        ):
            hash_ = None

        if hash_:
            return self.PARAM_CHECKSUM, hash_

        if self.isdir(path_info):
            typ, hash_ = self.get_dir_hash(path_info, **kwargs)
        else:
            typ, hash_ = self.get_file_hash(path_info)

        if hash_ and self.exists(path_info):
            self.state.save(path_info, hash_)

        return typ, hash_

    def get_file_hash(self, path_info):
        raise NotImplementedError

    def get_dir_hash(self, path_info, **kwargs):
        if not self.cache:
            raise RemoteCacheRequiredError(path_info)

        dir_info = self._collect_dir(path_info, **kwargs)
        return self.save_dir_info(dir_info)

    def hash_to_path_info(self, hash_):
        return self.path_info / hash_[0:2] / hash_[2:]

    def path_to_hash(self, path):
        parts = self.PATH_CLS(path).parts[-2:]

        if not (len(parts) == 2 and parts[0] and len(parts[0]) == 2):
            raise ValueError(f"Bad cache file path '{path}'")

        return "".join(parts)

    def save_info(self, path_info, **kwargs):
        typ, hash_ = self.get_hash(path_info, **kwargs)
        return {typ: hash_}

    def _calculate_hashes(self, file_infos):
        file_infos = list(file_infos)
        with Tqdm(
            total=len(file_infos),
            unit="md5",
            desc="Computing file/dir hashes (only done once)",
        ) as pbar:
            worker = pbar.wrap_fn(self.get_file_hash)
            with ThreadPoolExecutor(max_workers=self.hash_jobs) as executor:
                hashes = (
                    value for typ, value in executor.map(worker, file_infos)
                )
                return dict(zip(file_infos, hashes))

    def _collect_dir(self, path_info, **kwargs):
        file_infos = set()

        for fname in self.walk_files(path_info, **kwargs):
            if DvcIgnore.DVCIGNORE_FILE == fname.name:
                raise DvcIgnoreInCollectedDirError(fname.parent)

            file_infos.add(fname)

        hashes = {fi: self.state.get(fi) for fi in file_infos}
        not_in_state = {fi for fi, hash_ in hashes.items() if hash_ is None}

        new_hashes = self._calculate_hashes(not_in_state)
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

    def save_dir_info(self, dir_info):
        typ, hash_, tmp_info = self._get_dir_info_hash(dir_info)
        new_info = self.cache.tree.hash_to_path_info(hash_)
        if self.cache.changed_cache_file(hash_):
            self.cache.tree.makedirs(new_info.parent)
            self.cache.tree.move(
                tmp_info, new_info, mode=self.cache.CACHE_MODE
            )

        self.state.save(new_info, hash_)

        return typ, hash_

    def _get_dir_info_hash(self, dir_info):
        # Sorting the list by path to ensure reproducibility
        dir_info = sorted(dir_info, key=itemgetter(self.PARAM_RELPATH))

        tmp = tempfile.NamedTemporaryFile(delete=False).name
        with open(tmp, "w+") as fobj:
            json.dump(dir_info, fobj, sort_keys=True)

        tree = self.cache.tree
        from_info = PathInfo(tmp)
        to_info = tree.path_info / tmp_fname("")
        tree.upload(from_info, to_info, no_progress_bar=True)

        typ, hash_ = tree.get_file_hash(to_info)
        return typ, hash_ + self.CHECKSUM_DIR_SUFFIX, to_info

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
            prefix = True
        else:
            path_info = self.path_info
            prefix = False
        if progress_callback:
            for file_info in self.walk_files(path_info, prefix=prefix):
                progress_callback()
                yield file_info.path
        else:
            yield from self.walk_files(path_info, prefix=prefix)

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
