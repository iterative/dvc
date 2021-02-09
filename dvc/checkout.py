import logging

import dvc.prompt as prompt
from dvc.exceptions import CheckoutError, ConfirmRemoveError
from dvc.objects import ObjectFormatError, check, load

logger = logging.getLogger(__name__)


def _changed(path_info, tree, obj, cache):
    logger.trace("checking if '%s'('%s') has changed.", path_info, obj)

    try:
        check(cache, obj)
    except (FileNotFoundError, ObjectFormatError):
        logger.debug(
            "cache for '%s'('%s') has changed.", path_info, obj.hash_info
        )
        return True

    try:
        actual = tree.get_hash(path_info)
    except FileNotFoundError:
        logger.debug("'%s' doesn't exist.", path_info)
        return True

    if obj.hash_info != actual:
        logger.debug(
            "hash value '%s' for '%s' has changed (actual '%s').",
            obj.hash_info,
            actual,
            path_info,
        )
        return True

    logger.trace("'%s' hasn't changed.", path_info)
    return False


def _remove(path_info, tree, cache, force=False):
    if not tree.exists(path_info):
        return

    if force:
        tree.remove(path_info)
        return

    current = tree.get_hash(path_info)
    try:
        obj = load(cache, current)
        check(cache, obj)
    except (FileNotFoundError, ObjectFormatError):
        msg = (
            f"file/directory '{path_info}' is going to be removed. "
            "Are you sure you want to proceed?"
        )

        if not prompt.confirm(msg):
            raise ConfirmRemoveError(str(path_info))

    tree.remove(path_info)


def _checkout_file(
    path_info, tree, obj, cache, force, progress_callback=None, relink=False,
):
    """The file is changed we need to checkout a new copy"""
    modified = False
    cache_info = cache.tree.hash_to_path_info(obj.hash_info.value)
    if tree.exists(path_info):
        if not relink and _changed(path_info, tree, obj, cache):
            modified = True
            _remove(path_info, tree, cache, force=force)
            cache.link(cache_info, path_info)
        else:
            if tree.iscopy(path_info) and cache.cache_is_copy(path_info):
                cache.unprotect(path_info)
            else:
                _remove(path_info, tree, cache, force=force)
                cache.link(cache_info, path_info)
    else:
        cache.link(cache_info, path_info)
        modified = True

    tree.state.save(path_info, obj.hash_info)
    if progress_callback:
        progress_callback(str(path_info))

    return modified


def _remove_redundant_files(path_info, tree, dir_info, cache, force):
    existing_files = set(tree.walk_files(path_info))

    needed_files = {info for info, _ in dir_info.items(path_info)}
    redundant_files = existing_files - needed_files
    for path in redundant_files:
        _remove(path, tree, cache, force)

    return bool(redundant_files)


def _checkout_dir(
    path_info, tree, obj, cache, force, progress_callback=None, relink=False,
):
    modified = False, False
    # Create dir separately so that dir is created
    # even if there are no files in it
    if not tree.exists(path_info):
        modified = True
        tree.makedirs(path_info)

    logger.debug("Linking directory '%s'.", path_info)

    for entry_info, entry_hash_info in obj.hash_info.dir_info.items(path_info):
        entry_modified = _checkout_file(
            entry_info,
            tree,
            cache.get(entry_hash_info),
            cache,
            force,
            progress_callback,
            relink,
        )
        if entry_modified:
            modified = True

    modified = (
        _remove_redundant_files(
            path_info, tree, obj.hash_info.dir_info, cache, force
        )
        or modified
    )

    tree.state.save(path_info, obj.hash_info)

    # relink is not modified, assume it as nochange
    return modified and not relink


def _checkout(
    path_info,
    tree,
    obj,
    cache,
    force=False,
    progress_callback=None,
    relink=False,
):
    if not obj.hash_info.isdir:
        ret = _checkout_file(
            path_info, tree, obj, cache, force, progress_callback, relink
        )
    else:
        ret = _checkout_dir(
            path_info, tree, obj, cache, force, progress_callback, relink,
        )

    tree.state.save_link(path_info)

    return ret


def checkout(
    path_info,
    tree,
    obj,
    cache,
    force=False,
    progress_callback=None,
    relink=False,
    quiet=False,
):
    if path_info.scheme not in ["local", cache.tree.scheme]:
        raise NotImplementedError

    failed = None
    skip = False
    if not obj:
        if not quiet:
            logger.warning(
                "No file hash info found for '%s'. It won't be created.",
                path_info,
            )
        _remove(path_info, tree, cache, force=force)
        failed = path_info

    elif not relink and not _changed(path_info, tree, obj, cache):
        logger.trace("Data '%s' didn't change.", path_info)
        skip = True
    else:
        try:
            check(cache, obj)
        except (FileNotFoundError, ObjectFormatError):
            if not quiet:
                logger.warning(
                    "Cache '%s' not found. File '%s' won't be created.",
                    obj.hash_info,
                    path_info,
                )
            _remove(path_info, tree, cache, force=force)
            failed = path_info

    if failed or skip:
        if progress_callback and obj:
            progress_callback(
                str(path_info), len(obj),
            )
        if failed:
            raise CheckoutError([failed])
        return

    logger.debug("Checking out '%s' with cache '%s'.", path_info, obj)

    return _checkout(
        path_info, tree, obj, cache, force, progress_callback, relink,
    )
