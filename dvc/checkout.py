import logging

import dvc.prompt as prompt
from dvc.dir_info import DirInfo
from dvc.exceptions import CheckoutError, ConfirmRemoveError

logger = logging.getLogger(__name__)


def _filter_hash_info(cache, hash_info, path_info, filter_info):
    if not filter_info or path_info == filter_info:
        return hash_info

    dir_info = cache.get_dir_cache(hash_info)
    hash_key = filter_info.relative_to(path_info).parts

    # Check whether it is a file that exists on the trie
    hi = dir_info.trie.get(hash_key)
    if hi:
        return hi

    depth = len(hash_key)
    filtered_dir_info = DirInfo()
    try:
        for key, value in dir_info.trie.items(hash_key):
            filtered_dir_info.trie[key[depth:]] = value
    except KeyError:
        return None

    return cache.get_dir_info_hash(filtered_dir_info)[0]


def _changed(path_info, tree, hash_info, cache, filter_info=None):
    """Checks if data has changed.

    A file is considered changed if:
        - It doesn't exist on the working directory (was unlinked)
        - Hash value is not computed (saving a new file)
        - The hash value stored is different from the given one
        - There's no file in the cache

    Args:
        path_info: dict with path information.
        hash: expected hash value for this data.
        filter_info: an optional argument to target a specific path.

    Returns:
        bool: True if data has changed, False otherwise.
    """

    path = filter_info or path_info
    logger.trace("checking if '%s'('%s') has changed.", path_info, hash_info)

    if not tree.exists(path):
        logger.debug("'%s' doesn't exist.", path)
        return True

    hi = _filter_hash_info(cache, hash_info, path_info, filter_info)
    if not hi:
        logger.debug("hash value for '%s' is missing.", path)
        return True

    if cache.changed_cache(hi):
        logger.debug("cache for '%s'('%s') has changed.", path, hi)
        return True

    try:
        actual = tree.get_hash(path)
    except FileNotFoundError:
        actual = None
    if hi != actual:
        logger.debug(
            "hash value '%s' for '%s' has changed (actual '%s').",
            hi,
            actual,
            path,
        )
        return True

    logger.trace("'%s' hasn't changed.", path)
    return False


def _is_cached(cache, path_info, tree):
    current = tree.get_hash(path_info)

    if not current:
        return False

    return not cache.changed_cache(current)


def _remove(path_info, tree, cache, force=False):
    if not tree.exists(path_info):
        return

    if not force and not _is_cached(cache, path_info, tree):
        msg = (
            "file '{}' is going to be removed."
            " Are you sure you want to proceed?".format(str(path_info))
        )

        if not prompt.confirm(msg):
            raise ConfirmRemoveError(str(path_info))

    tree.remove(path_info)


def _checkout_file(
    path_info,
    tree,
    hash_info,
    cache,
    force,
    progress_callback=None,
    relink=False,
):
    """The file is changed we need to checkout a new copy"""
    cache_info = cache.tree.hash_to_path_info(hash_info.value)
    if tree.exists(path_info):
        added = False

        if not relink and _changed(path_info, tree, hash_info, cache):
            modified = True
            _remove(path_info, tree, cache, force=force)
            cache.link(cache_info, path_info)
        else:
            modified = False

            if tree.iscopy(path_info) and cache.cache_is_copy(path_info):
                cache.unprotect(path_info)
            else:
                _remove(path_info, tree, cache, force=force)
                cache.link(cache_info, path_info)
    else:
        cache.link(cache_info, path_info)
        added, modified = True, False

    tree.state.save(path_info, hash_info)
    if progress_callback:
        progress_callback(str(path_info))

    return added, modified


def _remove_redundant_files(path_info, tree, dir_info, cache, force):
    existing_files = set(tree.walk_files(path_info))

    needed_files = {info for info, _ in dir_info.items(path_info)}
    redundant_files = existing_files - needed_files
    for path in redundant_files:
        _remove(path, tree, cache, force)

    return bool(redundant_files)


def _checkout_dir(
    path_info,
    tree,
    hash_info,
    cache,
    force,
    progress_callback=None,
    relink=False,
    filter_info=None,
):
    added, modified = False, False
    # Create dir separately so that dir is created
    # even if there are no files in it
    if not tree.exists(path_info):
        added = True
        tree.makedirs(path_info)

    dir_info = cache.get_dir_cache(hash_info)

    logger.debug("Linking directory '%s'.", path_info)

    for entry_info, entry_hash_info in dir_info.items(path_info):
        if filter_info and not entry_info.isin_or_eq(filter_info):
            continue

        entry_added, entry_modified = _checkout_file(
            entry_info,
            tree,
            entry_hash_info,
            cache,
            force,
            progress_callback,
            relink,
        )
        if entry_added or entry_modified:
            modified = True

    modified = (
        _remove_redundant_files(path_info, tree, dir_info, cache, force)
        or modified
    )

    tree.state.save(path_info, hash_info)

    # relink is not modified, assume it as nochange
    return added, not added and modified and not relink


def _checkout(
    path_info,
    tree,
    hash_info,
    cache,
    force=False,
    progress_callback=None,
    relink=False,
    filter_info=None,
):
    if not hash_info.isdir:
        ret = _checkout_file(
            path_info, tree, hash_info, cache, force, progress_callback, relink
        )
    else:
        ret = _checkout_dir(
            path_info,
            tree,
            hash_info,
            cache,
            force,
            progress_callback,
            relink,
            filter_info,
        )

    tree.state.save_link(path_info)

    return ret


def checkout(
    path_info,
    tree,
    hash_info,
    cache,
    force=False,
    progress_callback=None,
    relink=False,
    filter_info=None,
    quiet=False,
):
    if path_info.scheme not in ["local", cache.tree.scheme]:
        raise NotImplementedError

    failed = None
    skip = False
    if not hash_info:
        if not quiet:
            logger.warning(
                "No file hash info found for '%s'. It won't be created.",
                path_info,
            )
        _remove(path_info, tree, cache, force=force)
        failed = path_info

    elif not relink and not _changed(
        path_info, tree, hash_info, cache, filter_info=filter_info
    ):
        logger.trace("Data '%s' didn't change.", path_info)
        skip = True

    elif cache.changed_cache(
        hash_info, path_info=path_info, filter_info=filter_info
    ):
        if not quiet:
            logger.warning(
                "Cache '%s' not found. File '%s' won't be created.",
                hash_info,
                path_info,
            )
        _remove(path_info, tree, cache, force=force)
        failed = path_info

    if failed or skip:
        if progress_callback:
            progress_callback(
                str(path_info),
                cache.get_files_number(path_info, hash_info, filter_info),
            )
        if failed:
            raise CheckoutError([failed])
        return

    logger.debug("Checking out '%s' with cache '%s'.", path_info, hash_info)

    return _checkout(
        path_info,
        tree,
        hash_info,
        cache,
        force,
        progress_callback,
        relink,
        filter_info,
    )
