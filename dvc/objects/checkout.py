import errno
import logging
from itertools import chain

from shortuuid import uuid

from dvc import prompt
from dvc.exceptions import (
    CacheLinkError,
    CheckoutError,
    ConfirmRemoveError,
    DvcException,
)
from dvc.ignore import DvcIgnoreFilter
from dvc.objects.db.slow_link_detection import (  # type: ignore[attr-defined]
    slow_link_guard,
)
from dvc.objects.diff import ROOT
from dvc.objects.diff import diff as odiff
from dvc.objects.stage import stage
from dvc.types import Optional

logger = logging.getLogger(__name__)


def _remove(path_info, fs, in_cache, force=False):
    if not fs.exists(path_info):
        return

    if force:
        fs.remove(path_info)
        return

    if not in_cache:
        msg = (
            f"file/directory '{path_info}' is going to be removed. "
            "Are you sure you want to proceed?"
        )

        if not prompt.confirm(msg):
            raise ConfirmRemoveError(str(path_info))

    fs.remove(path_info)


def _verify_link(cache, path_info, link_type):
    if link_type == "hardlink" and cache.fs.getsize(path_info) == 0:
        return

    if cache.cache_type_confirmed:
        return

    is_link = getattr(cache.fs, f"is_{link_type}", None)
    if is_link and not is_link(path_info):
        cache.fs.remove(path_info)
        raise DvcException(f"failed to verify {link_type}")

    cache.cache_type_confirmed = True


def _do_link(cache, from_info, to_info, link_method):
    if cache.fs.exists(to_info):
        cache.fs.remove(to_info)  # broken symlink

    link_method(from_info, to_info)

    logger.debug(
        "Created '%s': %s -> %s", cache.cache_types[0], from_info, to_info
    )


@slow_link_guard
def _try_links(cache, from_info, to_info, link_types):
    while link_types:
        link_method = getattr(cache.fs, link_types[0])
        try:
            _do_link(cache, from_info, to_info, link_method)
            _verify_link(cache, to_info, link_types[0])
            return

        except OSError as exc:
            if exc.errno not in [errno.EXDEV, errno.ENOTSUP]:
                raise
            logger.debug(
                "Cache type '%s' is not supported: %s", link_types[0], exc
            )
            del link_types[0]

    raise CacheLinkError([to_info])


def _link(cache, from_info, to_info):
    cache.makedirs(to_info.parent)
    try:
        _try_links(cache, from_info, to_info, cache.cache_types)
    except FileNotFoundError as exc:
        raise CheckoutError([str(to_info)]) from exc


def _confirm_cache_type(cache, path_info):
    """Checks whether cache uses copies."""
    if cache.cache_type_confirmed:
        return

    if set(cache.cache_types) <= {"copy"}:
        return

    workspace_file = path_info.with_name("." + uuid())
    test_cache_file = cache.path_info / ".cache_type_test_file"
    if not cache.fs.exists(test_cache_file):
        cache.makedirs(test_cache_file.parent)
        with cache.fs.open(test_cache_file, "wb") as fobj:
            fobj.write(bytes(1))
    try:
        _link(cache, test_cache_file, workspace_file)
    finally:
        cache.fs.remove(workspace_file)
        cache.fs.remove(test_cache_file)

    cache.cache_type_confirmed = True


def _relink(cache, cache_info, fs, path_info, in_cache, force):
    _remove(path_info, fs, in_cache, force=force)
    _link(cache, cache_info, path_info)
    # NOTE: Depending on a file system (e.g. on NTFS), `_remove` might reset
    # read-only permissions in order to delete a hardlink to protected object,
    # which will also reset it for the object itself, making it unprotected,
    # so we need to protect it back.
    cache.protect(cache_info)


def _checkout_file(
    path_info,
    fs,
    change,
    cache,
    force,
    progress_callback=None,
    relink=False,
    state=None,
):
    """The file is changed we need to checkout a new copy"""
    modified = False

    _confirm_cache_type(cache, path_info)

    cache_info = cache.hash_to_path_info(change.new.oid.value)
    if change.old.oid:
        if relink:
            if fs.iscopy(path_info) and cache.cache_types[0] == "copy":
                cache.unprotect(path_info)
            else:
                _relink(
                    cache,
                    cache_info,
                    fs,
                    path_info,
                    change.old.in_cache,
                    force=force,
                )
        else:
            modified = True
            _relink(
                cache,
                cache_info,
                fs,
                path_info,
                change.old.in_cache,
                force=force,
            )
    else:
        _link(cache, cache_info, path_info)
        modified = True

    if state:
        state.save(path_info, fs, change.new.oid)

    if progress_callback:
        progress_callback(str(path_info))

    return modified


def _diff(
    path_info,
    fs,
    obj,
    cache,
    relink=False,
    dvcignore: Optional[DvcIgnoreFilter] = None,
):
    old = None
    try:
        _, _, old = stage(
            cache,
            path_info,
            fs,
            obj.hash_info.name if obj else cache.fs.PARAM_CHECKSUM,
            dry_run=True,
            dvcignore=dvcignore,
        )
    except FileNotFoundError:
        pass

    diff = odiff(old, obj, cache)

    if relink:
        diff.modified.extend(diff.unchanged)

    return diff


def _checkout(
    diff,
    path_info,
    fs,
    cache,
    force=False,
    progress_callback=None,
    relink=False,
    state=None,
):
    if not diff:
        return

    for change in diff.deleted:
        entry_path = (
            path_info.joinpath(*change.old.key)
            if change.old.key != ROOT
            else path_info
        )
        _remove(entry_path, fs, change.old.in_cache, force=force)

    failed = []
    for change in chain(diff.added, diff.modified):
        entry_path = (
            path_info.joinpath(*change.new.key)
            if change.new.key != ROOT
            else path_info
        )
        if change.new.oid.isdir:
            fs.makedirs(entry_path)
            continue

        try:
            _checkout_file(
                entry_path,
                fs,
                change,
                cache,
                force,
                progress_callback,
                relink,
                state=state,
            )
        except CheckoutError as exc:
            failed.extend(exc.target_infos)

    if failed:
        raise CheckoutError(failed)


def checkout(
    path_info,
    fs,
    obj,
    cache,
    force=False,
    progress_callback=None,
    relink=False,
    quiet=False,
    dvcignore: Optional[DvcIgnoreFilter] = None,
    state=None,
):

    if path_info.scheme not in ["local", cache.fs.scheme]:
        raise NotImplementedError

    diff = _diff(
        path_info,
        fs,
        obj,
        cache,
        relink=relink,
        dvcignore=dvcignore,
    )

    failed = []
    if not obj:
        if not quiet:
            logger.warning(
                "No file hash info found for '%s'. It won't be created.",
                path_info,
            )
        failed.append(str(path_info))
    elif not diff:
        logger.trace("Data '%s' didn't change.", path_info)  # type: ignore

    try:
        _checkout(
            diff,
            path_info,
            fs,
            cache,
            force=force,
            progress_callback=progress_callback,
            relink=relink,
            state=state,
        )
    except CheckoutError as exc:
        failed.extend(exc.target_infos)

    if diff and state:
        state.save_link(path_info, fs)
        if not failed:
            state.save(path_info, fs, obj.hash_info)

    if failed or not diff:
        if progress_callback and obj:
            progress_callback(str(path_info), len(obj))
        if failed:
            raise CheckoutError(failed)
        return

    return bool(diff) and not relink
