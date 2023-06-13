import logging
import os
from functools import partial
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, List, Set, Tuple, Union

if TYPE_CHECKING:
    from dvc.dependency.base import Dependency
    from dvc.repo import Repo
    from dvc.repo.index import Index, IndexView
    from dvc.stage import Stage
    from dvc.types import TargetType
    from dvc_data.hashfile.hash_info import HashInfo


logger = logging.getLogger(__name__)


def unfetched_view(
    index: "Index", targets: "TargetType", unpartial: bool = False, **kwargs
) -> Tuple["IndexView", "IndexView", List["Dependency"]]:
    """Return index view of imports which have not been fetched.

    Returns:
        Tuple in the form (legacy_view, view, changed_deps) where changed_imports is a
        list of import dependencies that cannot be fetched due to changed data source.
    """
    from dvc.cachemgr import LEGACY_HASH_NAMES

    changed_deps: List["Dependency"] = []

    def need_fetch(stage: "Stage", legacy: bool = False) -> bool:
        if not stage.is_import or (stage.is_partial_import and not unpartial):
            return False

        out = stage.outs[0]
        if not out.changed_cache():
            return False

        dep = stage.deps[0]
        if dep.changed_checksum():
            changed_deps.append(dep)
            return False

        if out.hash_name in LEGACY_HASH_NAMES and legacy:
            return True
        if out.hash_name not in LEGACY_HASH_NAMES and not legacy:
            return True
        return False

    legacy_unfetched = index.targets_view(
        targets,
        stage_filter=partial(need_fetch, legacy=True),
        **kwargs,
    )
    unfetched = index.targets_view(targets, stage_filter=need_fetch, **kwargs)
    return legacy_unfetched, unfetched, changed_deps


def partial_view(index: "Index", targets: "TargetType", **kwargs) -> "IndexView":
    return index.targets_view(
        targets,
        stage_filter=lambda s: s.is_partial_import,
        **kwargs,
    )


def unpartial_imports(index: Union["Index", "IndexView"]) -> int:
    """Update any outs in the index which are no longer partial imports.

    Returns:
        Total number of files which were unpartialed.
    """
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_data.hashfile.meta import Meta

    updated = 0
    for out in index.outs:
        # we need to use view[key] here and since the out fields have not been
        # updated yet (out.get_entry() would return the partial-import state)
        workspace, key = out.index_key
        entry = index.data[workspace][key]
        if out.stage.is_partial_import:
            out.hash_info = entry.hash_info or HashInfo()
            out.meta = entry.meta or Meta()
            out.stage.md5 = out.stage.compute_md5()
            out.stage.dump()
            updated += out.meta.nfiles if out.meta.nfiles is not None else 1
    return updated


def save_imports(
    repo: "Repo", targets: "TargetType", unpartial: bool = False, **kwargs
) -> Set["HashInfo"]:
    """Save (download) imports from their original source location.

    Imports which are already cached will not be downloaded.

    Returns:
        Objects which were downloaded from source location.
    """
    from dvc.stage.exceptions import DataSourceChanged
    from dvc_data.index import md5, save
    from dvc_data.index.checkout import apply, compare
    from dvc_objects.fs.callbacks import Callback

    downloaded: Set["HashInfo"] = set()

    legacy_unfetched, unfetched, changed = unfetched_view(
        repo.index, targets, unpartial=unpartial, **kwargs
    )
    for dep in changed:
        logger.warning(str(DataSourceChanged(f"{dep.stage} ({dep})")))

    for view, cache in [
        (legacy_unfetched, repo.cache.legacy),
        (unfetched, repo.cache.local),
    ]:
        data_view = view.data["repo"]
        if len(data_view):
            if not cache.fs.exists(cache.path):
                os.makedirs(cache.path)
            with TemporaryDirectory(dir=cache.path) as tmpdir:
                with Callback.as_tqdm_callback(
                    desc="Downloading imports from source",
                    unit="files",
                ) as cb:
                    diff = compare(None, data_view)
                    apply(diff, tmpdir, cache.fs, callback=cb, storage="remote")
                md5(data_view, name=cache.hash_name)
                save(data_view, odb=cache, hardlink=True)

            downloaded.update(
                entry.hash_info
                for _, entry in data_view.iteritems()
                if entry.meta is not None
                and not entry.meta.isdir
                and entry.hash_info is not None
            )

    if unpartial:
        unpartial_imports(partial_view(repo.index, targets, **kwargs))

    return downloaded
