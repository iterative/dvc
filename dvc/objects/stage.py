from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Optional

from dvc.exceptions import DvcIgnoreInCollectedDirError
from dvc.hash_info import HashInfo
from dvc.ignore import DvcIgnore
from dvc.progress import Tqdm
from dvc.utils import file_md5

from .file import HashFile

if TYPE_CHECKING:
    from dvc.fs.base import BaseFileSystem
    from dvc.types import DvcPath

    from .db.base import ObjectDB


_STAGING_DIR = "staging"
_STAGING_MEMFS_PATH = f"dvc-{_STAGING_DIR}"


def _upload_file(path_info, fs, odb):
    from dvc.utils import tmp_fname
    from dvc.utils.stream import HashedStreamReader

    tmp_info = odb.path_info / tmp_fname()
    with fs.open(path_info, mode="rb", chunk_size=fs.CHUNK_SIZE) as stream:
        stream = HashedStreamReader(stream)
        odb.fs.upload_fobj(
            stream, tmp_info, desc=path_info.name, size=fs.getsize(path_info)
        )

    obj = HashFile(tmp_info, odb.fs, stream.hash_info)
    return path_info, obj


def _get_file_hash(path_info, fs, name):
    info = fs.info(path_info)
    if name in info:
        assert not info[name].endswith(".dir")
        return HashInfo(name, info[name], size=info["size"])

    func = getattr(fs, name, None)
    if func:
        return func(path_info)

    if name == "md5":
        return HashInfo(
            name, file_md5(path_info, fs), size=fs.getsize(path_info)
        )

    raise NotImplementedError


def get_file_hash(path_info, fs, name, state=None):
    if state:
        hash_info = state.get(  # pylint: disable=assignment-from-none
            path_info, fs
        )
        if hash_info:
            return hash_info

    hash_info = _get_file_hash(path_info, fs, name)

    if state:
        assert ".dir" not in hash_info.value
        state.save(path_info, fs, hash_info)

    return hash_info


def _get_file_obj(path_info, fs, name, odb=None, upload=False):
    if upload:
        assert odb and name == "md5"
        return _upload_file(path_info, fs, odb)

    state = odb.state if odb else None
    obj = HashFile(
        path_info, fs, get_file_hash(path_info, fs, name, state=state)
    )
    return path_info, obj


def _build_objects(
    path_info,
    fs,
    name,
    odb=None,
    upload=False,
    dvcignore=None,
    **kwargs,
):
    if dvcignore:
        walk_iterator = dvcignore.walk_files(fs, path_info)
    else:
        walk_iterator = fs.walk_files(path_info)
    with Tqdm(
        unit="md5",
        desc="Computing file/dir hashes (only done once)",
        disable=kwargs.pop("no_progress_bar", False),
    ) as pbar:
        worker = pbar.wrap_fn(
            partial(
                _get_file_obj,
                fs=fs,
                name=name,
                odb=odb,
                upload=upload,
            )
        )
        with ThreadPoolExecutor(
            max_workers=kwargs.pop("jobs", fs.hash_jobs)
        ) as executor:
            yield from executor.map(worker, walk_iterator)


def _iter_objects(path_info, fs, name, upload=False, **kwargs):
    if not upload and name in fs.DETAIL_FIELDS:
        for details in fs.find(path_info, detail=True):
            file_info = path_info.replace(path=details["name"])
            hash_info = HashInfo(name, details[name], size=details.get("size"))
            yield file_info, HashFile(file_info, fs, hash_info)

        return None

    yield from _build_objects(path_info, fs, name, upload=upload, **kwargs)


def _build_tree(path_info, fs, name, **kwargs):
    from .tree import Tree

    tree = Tree(None, None, None)
    for file_info, obj in _iter_objects(path_info, fs, name, **kwargs):
        if DvcIgnore.DVCIGNORE_FILE == file_info.name:
            raise DvcIgnoreInCollectedDirError(file_info.parent)

        # NOTE: this is lossy transformation:
        #   "hey\there" -> "hey/there"
        #   "hey/there" -> "hey/there"
        # The latter is fine filename on Windows, which
        # will transform to dir/file on back transform.
        #
        # Yes, this is a BUG, as long as we permit "/" in
        # filenames on Windows and "\" on Unix
        tree.add(file_info.relative_to(path_info).parts, obj)
    tree.digest()
    return tree


def _get_tree_obj(path_info, fs, name, odb=None, **kwargs):
    from .tree import Tree

    value = fs.info(path_info).get(name)
    if odb and value:
        hash_info = HashInfo(name, value)
        try:
            tree = Tree.load(odb, hash_info)
            # NOTE: loaded entries are naive objects with hash_infos but no
            # path_info. For staging trees, obj.path_info should be relative
            # to the staging src `path_info` and src fs
            for key, entry in tree:
                entry.fs = fs
                entry.path_info = path_info.joinpath(*key)
            return tree
        except FileNotFoundError:
            pass

    tree = _build_tree(path_info, fs, name, odb=odb, **kwargs)
    return tree


def get_staging(odb: Optional["ObjectDB"] = None) -> "ObjectDB":
    """Return an ODB that can be used for staging objects.

    If odb.fs is local, .dvc/tmp/staging will be returned. Otherwise
    the the global (temporary) memfs ODB will be returned.
    """

    from dvc.fs.memory import MemoryFileSystem
    from dvc.path_info import CloudURLInfo, PathInfo
    from dvc.scheme import Schemes

    from .db import get_odb

    if odb and odb.fs.scheme == Schemes.LOCAL and odb.tmp_dir:
        fs = odb.fs
        path_info: "DvcPath" = PathInfo(odb.tmp_dir) / _STAGING_DIR
        config = odb.config
    else:
        fs = MemoryFileSystem()
        path_info = CloudURLInfo(f"{Schemes.MEMORY}://{_STAGING_MEMFS_PATH}")
        config = {}
    return get_odb(fs, path_info, **config)


def is_memfs_staging(odb: "ObjectDB"):
    staging = get_staging()
    return odb.fs == staging.fs and odb.path_info == staging.path_info


def _load_from_state(odb, staging, path_info, fs, name):
    from . import load
    from .errors import ObjectFormatError
    from .tree import Tree

    state = odb.state
    hash_info = state.get(path_info, fs)
    if hash_info:
        for odb_ in (odb, staging):
            if odb_.exists(hash_info):
                try:
                    obj = load(odb_, hash_info)
                    if isinstance(obj, Tree):
                        obj.hash_info.nfiles = len(obj)
                        for key, entry in obj:
                            entry.fs = fs
                            entry.path_info = path_info.joinpath(*key)
                    else:
                        obj.fs = fs
                        obj.path_info = path_info
                    assert obj.hash_info.name == name
                    obj.hash_info.size = hash_info.size
                    return obj
                except ObjectFormatError:
                    pass
    raise FileNotFoundError


def _stage_external_tree_info(odb, tree, name):
    # NOTE: used only for external outputs. Initial reasoning was to be
    # able to validate .dir files right in the workspace (e.g. check s3
    # etag), but could be dropped for manual validation with regular md5,
    # that would be universal for all clouds.
    assert odb and name != "md5"

    odb.add(tree.path_info, tree.fs, tree.hash_info)
    raw = odb.get(tree.hash_info)
    hash_info = get_file_hash(raw.path_info, raw.fs, name, state=odb.state)
    tree.path_info = raw.path_info
    tree.fs = raw.fs
    tree.hash_info.name = hash_info.name
    tree.hash_info.value = hash_info.value
    if not tree.hash_info.value.endswith(".dir"):
        tree.hash_info.value += ".dir"
    return tree


def stage(
    odb: Optional["ObjectDB"],
    path_info: "DvcPath",
    fs: "BaseFileSystem",
    name: str,
    upload: bool = False,
    **kwargs,
) -> "HashFile":
    assert path_info and path_info.scheme == fs.scheme

    details = fs.info(path_info)
    staging = get_staging(odb)
    if odb:
        try:
            return _load_from_state(odb, staging, path_info, fs, name)
        except FileNotFoundError:
            pass

    if details["type"] == "directory":
        obj = _get_tree_obj(
            path_info, fs, name, odb=odb, upload=upload, **kwargs
        )
        if name == "md5":
            if odb and odb.exists(obj.hash_info):
                raw = odb.get(obj.hash_info)
            else:
                staging.add(obj.path_info, obj.fs, obj.hash_info)
                raw = staging.get(obj.hash_info)
            # cleanup unneeded memfs tmpfile and return obj based on staging
            # ODB fs/path
            if obj.fs != raw.fs:
                obj.fs.remove(obj.path_info)
            obj.fs = raw.fs
            obj.path_info = raw.path_info
        else:
            obj = _stage_external_tree_info(odb, obj, name)
    else:
        _, obj = _get_file_obj(path_info, fs, name, odb=odb, upload=upload)

    if odb and odb.state and obj.hash_info:
        odb.state.save(path_info, fs, obj.hash_info)

    return obj
