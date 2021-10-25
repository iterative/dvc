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


def _remove(fs_path, fs, in_cache, force=False):
    if not fs.exists(fs_path):
        return

    if force:
        fs.remove(fs_path)
        return

    if not in_cache:
        msg = (
            f"file/directory '{fs_path}' is going to be removed. "
            "Are you sure you want to proceed?"
        )

        if not prompt.confirm(msg):
            raise ConfirmRemoveError(fs_path)

    fs.remove(fs_path)


def _verify_link(cache, fs_path, link_type):
    if link_type == "hardlink" and cache.fs.getsize(fs_path) == 0:
        return

    if cache.cache_type_confirmed:
        return

    is_link = getattr(cache.fs, f"is_{link_type}", None)
    if is_link and not is_link(fs_path):
        cache.fs.remove(fs_path)
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
    cache.makedirs(cache.fs.path.parent(to_info))
    try:
        _try_links(cache, from_info, to_info, cache.cache_types)
    except FileNotFoundError as exc:
        raise CheckoutError([str(to_info)]) from exc


def _confirm_cache_type(cache, fs_path):
    """Checks whether cache uses copies."""
    if cache.cache_type_confirmed:
        return

    if set(cache.cache_types) <= {"copy"}:
        return

    workspace_file = cache.fs.path.with_name(fs_path, "." + uuid())
    test_cache_file = cache.fs.path.join(
        cache.fs_path, ".cache_type_test_file"
    )
    if not cache.fs.exists(test_cache_file):
        cache.makedirs(cache.fs.path.parent(test_cache_file))
        with cache.fs.open(test_cache_file, "wb") as fobj:
            fobj.write(bytes(1))
    try:
        _link(cache, test_cache_file, workspace_file)
    finally:
        cache.fs.remove(workspace_file)
        cache.fs.remove(test_cache_file)

    cache.cache_type_confirmed = True


def _relink(cache, cache_info, fs, path, in_cache, force):
    _remove(path, fs, in_cache, force=force)
    _link(cache, cache_info, path)
    # NOTE: Depending on a file system (e.g. on NTFS), `_remove` might reset
    # read-only permissions in order to delete a hardlink to protected object,
    # which will also reset it for the object itself, making it unprotected,
    # so we need to protect it back.
    cache.protect(cache_info)


def _checkout_file(
    fs_path,
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
    _confirm_cache_type(cache, fs_path)

    cache_fs_path = cache.hash_to_path(change.new.oid.value)
    if change.old.oid:
        if relink:
            if fs.iscopy(fs_path) and cache.cache_types[0] == "copy":
                cache.unprotect(fs_path)
            else:
                _relink(
                    cache,
                    cache_fs_path,
                    fs,
                    fs_path,
                    change.old.in_cache,
                    force=force,
                )
        else:
            modified = True
            _relink(
                cache,
                cache_fs_path,
                fs,
                fs_path,
                change.old.in_cache,
                force=force,
            )
    else:
        _link(cache, cache_fs_path, fs_path)
        modified = True

    if state:
        state.save(fs_path, fs, change.new.oid)

    if progress_callback:
        progress_callback(fs_path)

    return modified


def _diff(
    fs_path,
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
            fs_path,
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
    fs_path,
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
            fs.path.join(fs_path, *change.old.key)
            if change.old.key != ROOT
            else fs_path
        )
        _remove(entry_path, fs, change.old.in_cache, force=force)

    failed = []
    for change in chain(diff.added, diff.modified):
        entry_path = (
            fs.path.join(fs_path, *change.new.key)
            if change.new.key != ROOT
            else fs_path
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
    fs_path,
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
    # if scheme(fs_path) not in ["local", cache.fs.scheme]:
    #    raise NotImplementedError

    diff = _diff(
        fs_path,
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
                fs_path,
            )
        failed.append(fs_path)
    elif not diff:
        logger.trace("Data '%s' didn't change.", fs_path)  # type: ignore

    try:
        _checkout(
            diff,
            fs_path,
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
        state.save_link(fs_path, fs)
        if not failed:
            state.save(fs_path, fs, obj.hash_info)

    if failed or not diff:
        if progress_callback and obj:
            progress_callback(fs_path, len(obj))
        if failed:
            raise CheckoutError(failed)
        return

    return bool(diff) and not relink
