import json
import logging
from copy import copy

from shortuuid import uuid

import dvc.prompt as prompt
from dvc.exceptions import (
    CheckoutError,
    ConfirmRemoveError,
    DvcException,
    MergeError,
)
from dvc.path_info import WindowsPathInfo
from dvc.progress import Tqdm
from dvc.remote.slow_link_detection import slow_link_guard

from ..tree.base import RemoteActionNotImplemented

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


class DirCacheError(DvcException):
    def __init__(self, hash_):
        super().__init__(
            f"Failed to load dir cache for hash value: '{hash_}'."
        )


class CloudCache:
    """Cloud cache class."""

    DEFAULT_CACHE_TYPES = ["copy"]
    CACHE_MODE = None

    def __init__(self, tree):
        self.tree = tree
        self.repo = tree.repo

        self.cache_types = tree.config.get("type") or copy(
            self.DEFAULT_CACHE_TYPES
        )
        self.cache_type_confirmed = False
        self._dir_info = {}

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
        path_info = self.tree.hash_to_path_info(hash_)

        try:
            with self.tree.open(path_info, "r") as fobj:
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

        typ, actual = self.tree.get_hash(path_info)
        assert typ == self.tree.PARAM_CHECKSUM

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

        cache_info = self.tree.hash_to_path_info(hash_)
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
                self.tree.state.save_link(path_info)
            # we need to update path and cache, since in case of reflink,
            # or copy cache type moving original file results in updates on
            # next executed command, which causes md5 recalculation
            self.tree.state.save(path_info, hash_)
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

        self.tree.state.save(cache_info, hash_)
        return {self.tree.PARAM_CHECKSUM: hash_}

    def _cache_is_copy(self, path_info):
        """Checks whether cache uses copies."""
        if self.cache_type_confirmed:
            return self.cache_types[0] == "copy"

        if set(self.cache_types) <= {"copy"}:
            return True

        workspace_file = path_info.with_name("." + uuid())
        test_cache_file = self.tree.path_info / ".cache_type_test_file"
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
            self.tree.state.save_link(path_info)
        if self.tree.exists(path_info):
            self.tree.state.save(path_info, hash_)

        cache_info = self.tree.hash_to_path_info(hash_)
        self.tree.state.save(cache_info, hash_)
        return {self.tree.PARAM_CHECKSUM: hash_}

    def save(self, path_info, tree, hash_info, save_link=True, **kwargs):
        if path_info.scheme != self.tree.scheme:
            raise RemoteActionNotImplemented(
                f"save {path_info.scheme} -> {self.tree.scheme}",
                self.tree.scheme,
            )

        hash_ = hash_info[self.tree.PARAM_CHECKSUM]
        return self._save(path_info, tree, hash_, save_link, **kwargs)

    def _save(self, path_info, tree, hash_, save_link=True, **kwargs):
        to_info = self.tree.hash_to_path_info(hash_)
        logger.debug("Saving '%s' to '%s'.", path_info, to_info)

        if tree.isdir(path_info):
            return self._save_dir(path_info, tree, hash_, save_link, **kwargs)
        return self._save_file(path_info, tree, hash_, save_link, **kwargs)

    # Override to return path as a string instead of PathInfo for clouds
    # which support string paths (see local)
    def hash_to_path(self, hash_):
        return self.tree.hash_to_path_info(hash_)

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

        _, actual = self.tree.get_hash(cache_info)

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
        if self.tree.is_dir_hash(hash_):
            return self._changed_dir_cache(
                hash_, path_info=path_info, filter_info=filter_info
            )
        return self.changed_cache_file(hash_)

    def already_cached(self, path_info):
        _, current = self.tree.get_hash(path_info)

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
        cache_info = self.tree.hash_to_path_info(hash_)
        if self.tree.exists(path_info):
            logger.debug("data '%s' will be replaced.", path_info)
            self.safe_remove(path_info, force=force)
            added, modified = False, True

        self.link(cache_info, path_info)
        self.tree.state.save_link(path_info)
        self.tree.state.save(path_info, hash_)
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
            entry_cache_info = self.tree.hash_to_path_info(entry_hash)
            entry_info = path_info / relative_path

            if filter_info and not entry_info.isin_or_eq(filter_info):
                continue

            entry_hash_info = {self.tree.PARAM_CHECKSUM: entry_hash}
            if relink or self.changed(entry_info, entry_hash_info):
                modified = True
                self.safe_remove(entry_info, force=force)
                self.link(entry_cache_info, entry_info)
                self.tree.state.save(entry_info, entry_hash)
            if progress_callback:
                progress_callback(str(entry_info))

        modified = (
            self._remove_redundant_files(path_info, dir_info, force)
            or modified
        )

        self.tree.state.save_link(path_info)
        self.tree.state.save(path_info, hash_)

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
        if path_info.scheme not in ["local", self.tree.scheme]:
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
                    self.get_files_number(
                        self.tree.path_info, hash_, filter_info
                    ),
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
        if not self.tree.is_dir_hash(hash_):
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

        if not self.tree.is_dir_hash(hash_):
            return 1

        if not filter_info:
            return len(self.get_dir_cache(hash_))

        return ilen(
            filter_info.isin_or_eq(path_info / entry[self.tree.PARAM_CHECKSUM])
            for entry in self.get_dir_cache(hash_)
        )

    def _to_dict(self, dir_info):
        return {
            entry[self.tree.PARAM_RELPATH]: entry[self.tree.PARAM_CHECKSUM]
            for entry in dir_info
        }

    def _from_dict(self, dir_dict):
        return [
            {
                self.tree.PARAM_RELPATH: relpath,
                self.tree.PARAM_CHECKSUM: checksum,
            }
            for relpath, checksum in dir_dict.items()
        ]

    @staticmethod
    def _diff(ancestor, other, allow_removed=False):
        from dictdiffer import diff

        allowed = ["add"]
        if allow_removed:
            allowed.append("remove")

        result = list(diff(ancestor, other))
        for typ, _, _ in result:
            if typ not in allowed:
                raise MergeError(
                    "unable to auto-merge directories with diff that contains "
                    f"'{typ}'ed files"
                )
        return result

    def _merge_dirs(self, ancestor_info, our_info, their_info):
        from operator import itemgetter

        from dictdiffer import patch

        ancestor = self._to_dict(ancestor_info)
        our = self._to_dict(our_info)
        their = self._to_dict(their_info)

        our_diff = self._diff(ancestor, our)
        if not our_diff:
            return self._from_dict(their)

        their_diff = self._diff(ancestor, their)
        if not their_diff:
            return self._from_dict(our)

        # make sure there are no conflicting files
        self._diff(our, their, allow_removed=True)

        merged = patch(our_diff + their_diff, ancestor, in_place=True)

        # Sorting the list by path to ensure reproducibility
        return sorted(
            self._from_dict(merged), key=itemgetter(self.tree.PARAM_RELPATH)
        )

    def merge(self, ancestor_info, our_info, their_info):
        assert our_info
        assert their_info

        if ancestor_info:
            ancestor_hash = ancestor_info[self.tree.PARAM_CHECKSUM]
            ancestor = self.get_dir_cache(ancestor_hash)
        else:
            ancestor = []

        our_hash = our_info[self.tree.PARAM_CHECKSUM]
        our = self.get_dir_cache(our_hash)

        their_hash = their_info[self.tree.PARAM_CHECKSUM]
        their = self.get_dir_cache(their_hash)

        merged = self._merge_dirs(ancestor, our, their)
        typ, merged_hash = self.tree.save_dir_info(merged)
        return {typ: merged_hash}
