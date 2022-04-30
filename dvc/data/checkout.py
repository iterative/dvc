import logging
from itertools import chain

from dvc import prompt
from dvc.exceptions import CacheLinkError, CheckoutError, ConfirmRemoveError
from dvc.fs.utils import test_links, transfer
from dvc.ignore import DvcIgnoreFilter
from dvc.types import Optional

from .diff import ROOT
from .diff import diff as odiff
from .slow_link_detection import slow_link_guard  # type: ignore[attr-defined]
from .stage import stage

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


def _relink(link, cache, cache_info, fs, path, in_cache, force):
    _remove(path, fs, in_cache, force=force)
    link(cache, cache_info, fs, path)
    # NOTE: Depending on a file system (e.g. on NTFS), `_remove` might reset
    # read-only permissions in order to delete a hardlink to protected object,
    # which will also reset it for the object itself, making it unprotected,
    # so we need to protect it back.
    cache.protect(cache_info)


def _checkout_file(
    link,
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

    cache_fs_path = cache.hash_to_path(change.new.oid.value)
    if change.old.oid:
        if relink:
            if fs.iscopy(fs_path) and cache.cache_types[0] == "copy":
                cache.unprotect(fs_path)
            else:
                _relink(
                    link,
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
                link,
                cache,
                cache_fs_path,
                fs,
                fs_path,
                change.old.in_cache,
                force=force,
            )
    else:
        link(cache, cache_fs_path, fs, fs_path)
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


class Link:
    def __init__(self, links):
        self._links = links

    @slow_link_guard
    def __call__(self, cache, from_path, to_fs, to_path):
        if to_fs.exists(to_path):
            to_fs.remove(to_path)  # broken symlink

        cache.makedirs(cache.fs.path.parent(to_path))
        try:
            transfer(cache.fs, from_path, to_fs, to_path, links=self._links)
        except FileNotFoundError as exc:
            raise CheckoutError([to_path]) from exc
        except OSError as exc:
            raise CacheLinkError([to_path]) from exc


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

    links = test_links(cache.cache_types, cache.fs, cache.fs_path, fs, fs_path)
    if not links:
        raise CacheLinkError([fs_path])
    link = Link(links)

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
                link,
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
