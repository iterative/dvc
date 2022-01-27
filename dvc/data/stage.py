import hashlib
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from dvc.exceptions import DvcIgnoreInCollectedDirError
from dvc.hash_info import HashInfo
from dvc.ignore import DvcIgnore
from dvc.objects.file import HashFile
from dvc.progress import Tqdm
from dvc.utils import file_md5, is_exec

from .db.reference import ReferenceObjectDB
from .meta import Meta

if TYPE_CHECKING:
    from dvc.fs.base import FileSystem
    from dvc.objects.db import ObjectDB
    from dvc.types import AnyPath

logger = logging.getLogger(__name__)


_STAGING_MEMFS_PATH = "dvc-staging"


def _upload_file(from_fs_path, fs, odb, upload_odb):
    from dvc.utils import tmp_fname
    from dvc.utils.stream import HashedStreamReader

    fs_path = upload_odb.fs.path
    tmp_info = fs_path.join(upload_odb.fs_path, tmp_fname())
    with fs.open(from_fs_path, mode="rb", chunk_size=fs.CHUNK_SIZE) as stream:
        stream = HashedStreamReader(stream)
        size = fs.getsize(from_fs_path)
        upload_odb.fs.upload(
            stream, tmp_info, desc=fs_path.name(from_fs_path), total=size
        )

    odb.add(tmp_info, upload_odb.fs, stream.hash_info)
    meta = Meta(size=size)
    return from_fs_path, meta, odb.get(stream.hash_info)


def _adapt_info(info, scheme):
    if scheme == "s3" and "ETag" in info:
        info["etag"] = info["ETag"].strip('"')
    elif scheme == "gs" and "etag" in info:
        import base64

        info["etag"] = base64.b64decode(info["etag"]).hex()
    elif scheme.startswith("http") and (
        "ETag" in info or "Content-MD5" in info
    ):
        info["checksum"] = info.get("ETag") or info.get("Content-MD5")
    return info


def _get_file_hash(fs_path, fs, name):
    info = _adapt_info(fs.info(fs_path), fs.scheme)

    if name in info:
        assert not info[name].endswith(".dir")
        hash_value = info[name]
    elif hasattr(fs, name):
        func = getattr(fs, name)
        hash_value = func(fs_path)
    elif name == "md5":
        hash_value = file_md5(fs_path, fs)
    else:
        raise NotImplementedError

    meta = Meta(size=info["size"], isexec=is_exec(info.get("mode", 0)))
    hash_info = HashInfo(name, hash_value)
    return meta, hash_info


def get_file_hash(fs_path, fs, name, state=None):
    if state:
        meta, hash_info = state.get(  # pylint: disable=assignment-from-none
            fs_path, fs
        )
        if hash_info:
            return meta, hash_info

    meta, hash_info = _get_file_hash(fs_path, fs, name)

    if state:
        assert ".dir" not in hash_info.value
        state.save(fs_path, fs, hash_info)

    return meta, hash_info


def _stage_file(fs_path, fs, name, odb=None, upload_odb=None, dry_run=False):
    state = odb.state if odb else None
    meta, hash_info = get_file_hash(fs_path, fs, name, state=state)
    if upload_odb and not dry_run:
        assert odb and name == "md5"
        return _upload_file(fs_path, fs, odb, upload_odb)

    if dry_run:
        obj = HashFile(fs_path, fs, hash_info)
    else:
        odb.add(fs_path, fs, hash_info, hardlink=False)
        obj = odb.get(hash_info)

    return fs_path, meta, obj


def _build_objects(
    fs_path,
    fs,
    name,
    dvcignore=None,
    jobs=None,
    no_progress_bar=False,
    **kwargs,
):
    if dvcignore:
        walk_iterator = dvcignore.find(fs, fs_path)
    else:
        walk_iterator = fs.find(fs_path)
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


def _iter_objects(fs_path, fs, name, **kwargs):
    yield from _build_objects(fs_path, fs, name, **kwargs)


def _build_tree(fs_path, fs, name, **kwargs):
    from .tree import Tree

    tree_meta = Meta(size=0, nfiles=0)
    tree = Tree(None, None, None)
    for file_fs_path, meta, obj in _iter_objects(fs_path, fs, name, **kwargs):
        if DvcIgnore.DVCIGNORE_FILE == fs.path.name(file_fs_path):
            raise DvcIgnoreInCollectedDirError(fs.path.parent(file_fs_path))

        # NOTE: this is lossy transformation:
        #   "hey\there" -> "hey/there"
        #   "hey/there" -> "hey/there"
        # The latter is fine filename on Windows, which
        # will transform to dir/file on back transform.
        #
        # Yes, this is a BUG, as long as we permit "/" in
        # filenames on Windows and "\" on Unix

        key = fs.path.relparts(file_fs_path, fs_path)
        assert key
        tree.add(key, meta, obj.hash_info)

        tree_meta.size += meta.size or 0
        tree_meta.nfiles += 1

    return tree_meta, tree


def _stage_tree(fs_path, fs, fs_info, name, odb=None, **kwargs):
    from .tree import Tree

    value = fs_info.get(name)
    if odb and value:
        hash_info = HashInfo(name, value)
        try:
            tree = Tree.load(odb, hash_info)
            return Meta(nfiles=len(tree)), tree
        except FileNotFoundError:
            pass

    meta, tree = _build_tree(fs_path, fs, name, odb=odb, **kwargs)
    state = odb.state if odb and odb.state else None
    hash_info = None
    if state:
        _, hash_info = state.get(  # pylint: disable=assignment-from-none
            fs_path, fs
        )
    tree.digest(hash_info=hash_info)
    odb.add(tree.fs_path, tree.fs, tree.hash_info, hardlink=False)
    raw = odb.get(tree.hash_info)
    # cleanup unneeded memfs tmpfile and return tree based on the
    # ODB fs/path
    if odb.fs != tree.fs:
        tree.fs.remove(tree.fs_path)
    tree.fs = raw.fs
    tree.fs_path = raw.fs_path
    return meta, tree


_url_cache: Dict[str, str] = {}


def _make_staging_url(
    fs: "FileSystem", odb: "ObjectDB", fs_path: Optional[str]
):
    from dvc.scheme import Schemes

    url = f"{Schemes.MEMORY}://{_STAGING_MEMFS_PATH}"

    if fs_path is not None:
        if odb.fs.scheme == Schemes.LOCAL:
            fs_path = os.path.abspath(fs_path)

        if fs_path not in _url_cache:
            _url_cache[fs_path] = hashlib.sha256(
                fs_path.encode("utf-8")
            ).hexdigest()

        url = fs.path.join(url, _url_cache[fs_path])

    return url


def _get_staging(odb: "ObjectDB") -> "ObjectDB":
    """Return an ODB that can be used for staging objects.

    Staging will be a reference ODB stored in the the global memfs.
    """

    from dvc.fs.memory import MemoryFileSystem

    fs = MemoryFileSystem()
    fs_path = _make_staging_url(fs, odb, odb.fs_path)
    state = odb.state
    return ReferenceObjectDB(fs, fs_path, state=state)


def _load_from_state(odb, staging, fs_path, fs, name):
    from dvc.objects.errors import ObjectFormatError

    from . import check, load
    from .tree import Tree

    state = odb.state
    meta, hash_info = state.get(fs_path, fs)
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

    odb.add(tree.fs_path, tree.fs, tree.hash_info)
    raw = odb.get(tree.hash_info)
    _, hash_info = get_file_hash(raw.fs_path, raw.fs, name, state=odb.state)
    tree.fs_path = raw.fs_path
    tree.fs = raw.fs
    tree.hash_info.name = hash_info.name
    tree.hash_info.value = hash_info.value
    if not tree.hash_info.value.endswith(".dir"):
        tree.hash_info.value += ".dir"
    return tree


def stage(
    odb: "ObjectDB",
    fs_path: "AnyPath",
    fs: "FileSystem",
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
    assert fs_path
    # assert scheme(fs_path) == fs.scheme

    details = fs.info(fs_path)
    staging = _get_staging(odb)
    if odb:
        try:
            return _load_from_state(odb, staging, fs_path, fs, name)
        except FileNotFoundError:
            pass

    if details["type"] == "directory":
        meta, obj = _stage_tree(
            fs_path,
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
            fs_path,
            fs,
            name,
            odb=staging,
            upload_odb=odb if upload else None,
            dry_run=dry_run,
        )

    if odb and odb.state and obj.hash_info:
        odb.state.save(fs_path, fs, obj.hash_info)

    return staging, meta, obj
