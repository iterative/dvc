import hashlib
import logging
import os
import pathlib
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Dict, Tuple

from dvc.exceptions import DvcIgnoreInCollectedDirError
from dvc.hash_info import HashInfo
from dvc.ignore import DvcIgnore
from dvc.progress import Tqdm
from dvc.utils import file_md5

from .db.reference import ReferenceObjectDB
from .file import HashFile
from .meta import Meta

if TYPE_CHECKING:
    from dvc.fs.base import BaseFileSystem
    from dvc.types import AnyPath, DvcPath

    from .db.base import ObjectDB

logger = logging.getLogger(__name__)


_STAGING_MEMFS_PATH = "dvc-staging"


def _upload_file(path_info, fs, odb, upload_odb):
    from dvc.utils import tmp_fname
    from dvc.utils.stream import HashedStreamReader

    tmp_info = upload_odb.path_info / tmp_fname()
    with fs.open(path_info, mode="rb", chunk_size=fs.CHUNK_SIZE) as stream:
        stream = HashedStreamReader(stream)
        size = fs.getsize(path_info)
        upload_odb.fs.upload(stream, tmp_info, desc=path_info.name, total=size)

    odb.add(tmp_info, upload_odb.fs, stream.hash_info)
    meta = Meta(size=size)
    return path_info, meta, odb.get(stream.hash_info)


def _get_file_hash(path_info, fs, name):
    info = fs.info(path_info)
    if name in info:
        assert not info[name].endswith(".dir")
        hash_value = info[name]
    elif hasattr(fs, name):
        func = getattr(fs, name)
        hash_value = func(path_info)
    elif name == "md5":
        hash_value = file_md5(path_info, fs)
    else:
        raise NotImplementedError

    meta = Meta(size=info["size"])
    hash_info = HashInfo(name, hash_value)
    return meta, hash_info


def get_file_hash(path_info, fs, name, state=None):
    if state:
        meta, hash_info = state.get(  # pylint: disable=assignment-from-none
            path_info, fs
        )
        if hash_info:
            return meta, hash_info

    meta, hash_info = _get_file_hash(path_info, fs, name)

    if state:
        assert ".dir" not in hash_info.value
        state.save(path_info, fs, hash_info)

    return meta, hash_info


def _stage_file(path_info, fs, name, odb=None, upload_odb=None, dry_run=False):
    state = odb.state if odb else None
    meta, hash_info = get_file_hash(path_info, fs, name, state=state)
    if upload_odb and not dry_run:
        assert odb and name == "md5"
        return _upload_file(path_info, fs, odb, upload_odb)

    if dry_run:
        obj = HashFile(path_info, fs, hash_info)
    else:
        odb.add(path_info, fs, hash_info, hardlink=False)
        obj = odb.get(hash_info)

    return path_info, meta, obj


def _build_objects(
    path_info,
    fs,
    name,
    dvcignore=None,
    jobs=None,
    no_progress_bar=False,
    **kwargs,
):
    if dvcignore:
        walk_iterator = dvcignore.walk_files(fs, path_info)
    else:
        walk_iterator = fs.walk_files(path_info)
    with Tqdm(
        unit="md5",
        desc="Computing file/dir hashes (only done once)",
        disable=no_progress_bar,
    ) as pbar:
        worker = pbar.wrap_fn(
            partial(
                _stage_file,
                fs=fs,
                name=name,
                **kwargs,
            )
        )
        with ThreadPoolExecutor(
            max_workers=jobs if jobs is not None else fs.hash_jobs
        ) as executor:
            yield from executor.map(worker, walk_iterator)


def _iter_objects(path_info, fs, name, **kwargs):
    yield from _build_objects(path_info, fs, name, **kwargs)


def _build_tree(path_info, fs, name, **kwargs):
    from .tree import Tree

    tree_meta = Meta(size=0, nfiles=0)
    tree = Tree(None, None, None)
    for file_info, meta, obj in _iter_objects(path_info, fs, name, **kwargs):
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
        tree.add(file_info.relative_to(path_info).parts, meta, obj.hash_info)

        tree_meta.size += meta.size
        tree_meta.nfiles += 1

    return tree_meta, tree


def _stage_tree(path_info, fs, fs_info, name, odb=None, **kwargs):
    from .tree import Tree

    value = fs_info.get(name)
    if odb and value:
        hash_info = HashInfo(name, value)
        try:
            tree = Tree.load(odb, hash_info)
            return Meta(nfiles=len(tree)), tree
        except FileNotFoundError:
            pass

    meta, tree = _build_tree(path_info, fs, name, odb=odb, **kwargs)
    state = odb.state if odb and odb.state else None
    hash_info = None
    if state:
        _, hash_info = state.get(  # pylint: disable=assignment-from-none
            path_info, fs
        )
    tree.digest(hash_info=hash_info)
    odb.add(tree.path_info, tree.fs, tree.hash_info, hardlink=False)
    raw = odb.get(tree.hash_info)
    # cleanup unneeded memfs tmpfile and return tree based on the
    # ODB fs/path
    if odb.fs != tree.fs:
        tree.fs.remove(tree.path_info)
    tree.fs = raw.fs
    tree.path_info = raw.path_info
    return meta, tree


_url_cache: Dict[str, str] = {}


def _make_staging_url(path_info: "AnyPath"):
    from dvc.path_info import CloudURLInfo
    from dvc.scheme import Schemes

    url = CloudURLInfo(f"{Schemes.MEMORY}://{_STAGING_MEMFS_PATH}")
    if path_info:
        if isinstance(path_info, (str, pathlib.PurePath)):
            path = os.path.abspath(path_info)
        else:
            path = str(path_info)
        if path not in _url_cache:
            _url_cache[path] = hashlib.sha256(path.encode("utf-8")).hexdigest()
        url /= _url_cache[path]
    return url


def _get_staging(odb: "ObjectDB") -> "ObjectDB":
    """Return an ODB that can be used for staging objects.

    Staging will be a reference ODB stored in the the global memfs.
    """

    from dvc.fs.memory import MemoryFileSystem

    fs = MemoryFileSystem()
    path_info = _make_staging_url(odb.path_info)
    state = odb.state
    return ReferenceObjectDB(fs, path_info, state=state)


def _load_from_state(odb, staging, path_info, fs, name):
    from . import check, load
    from .errors import ObjectFormatError
    from .tree import Tree

    state = odb.state
    meta, hash_info = state.get(path_info, fs)
    if hash_info:
        for odb_ in (odb, staging):
            if odb_.exists(hash_info):
                try:
                    obj = load(odb_, hash_info)
                    check(odb_, obj, check_hash=False)
                    if isinstance(obj, Tree):
                        meta.nfiles = len(obj)
                    assert obj.hash_info.name == name
                    return odb_, meta, obj
                except (ObjectFormatError, FileNotFoundError):
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
    _, hash_info = get_file_hash(raw.path_info, raw.fs, name, state=odb.state)
    tree.path_info = raw.path_info
    tree.fs = raw.fs
    tree.hash_info.name = hash_info.name
    tree.hash_info.value = hash_info.value
    if not tree.hash_info.value.endswith(".dir"):
        tree.hash_info.value += ".dir"
    return tree


def stage(
    odb: "ObjectDB",
    path_info: "DvcPath",
    fs: "BaseFileSystem",
    name: str,
    upload: bool = False,
    dry_run: bool = False,
    **kwargs,
) -> Tuple["ObjectDB", "Meta", "HashFile"]:
    """Stage (prepare) objects from the given path for addition to an ODB.

    Returns at tuple of (staging_odb, object) where addition to the ODB can
    be completed by transferring the object from staging to the dest ODB.

    If dry_run is True, object hashes will be computed and returned, but file
    objects themselves will not be added to the staging ODB (i.e. the resulting
    file objects cannot transferred from staging to another ODB).

    If upload is True, files will be uploaded to a temporary path on the dest
    ODB filesystem, and staged objects will reference the uploaded path rather
    than the original source path.
    """
    assert path_info and path_info.scheme == fs.scheme

    details = fs.info(path_info)
    staging = _get_staging(odb)
    if odb:
        try:
            return _load_from_state(odb, staging, path_info, fs, name)
        except FileNotFoundError:
            pass

    if details["type"] == "directory":
        meta, obj = _stage_tree(
            path_info,
            fs,
            details,
            name,
            odb=staging,
            upload_odb=odb if upload else None,
            dry_run=dry_run,
            **kwargs,
        )
        logger.debug("staged tree '%s'", obj)
        if name != "md5":
            obj = _stage_external_tree_info(odb, obj, name)
    else:
        _, meta, obj = _stage_file(
            path_info,
            fs,
            name,
            odb=staging,
            upload_odb=odb if upload else None,
            dry_run=dry_run,
        )

    if odb and odb.state and obj.hash_info:
        odb.state.save(path_info, fs, obj.hash_info)

    return staging, meta, obj
