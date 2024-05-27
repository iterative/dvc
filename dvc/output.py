import errno
import os
import posixpath
from collections import defaultdict
from contextlib import suppress
from operator import itemgetter
from typing import TYPE_CHECKING, Any, Optional, Union
from urllib.parse import urlparse

import voluptuous as vol
from funcy import collecting, first, project

from dvc import prompt
from dvc.exceptions import (
    CacheLinkError,
    CheckoutError,
    CollectCacheError,
    ConfirmRemoveError,
    DvcException,
    MergeError,
)
from dvc.log import logger
from dvc.utils import format_link
from dvc.utils.objects import cached_property
from dvc_data.hashfile import check as ocheck
from dvc_data.hashfile import load as oload
from dvc_data.hashfile.build import build
from dvc_data.hashfile.checkout import checkout
from dvc_data.hashfile.db import HashFileDB, add_update_tree
from dvc_data.hashfile.hash import DEFAULT_ALGORITHM
from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.istextfile import istextfile
from dvc_data.hashfile.meta import Meta
from dvc_data.hashfile.transfer import transfer as otransfer
from dvc_data.hashfile.tree import Tree, du
from dvc_objects.errors import ObjectFormatError

from .annotations import ANNOTATION_FIELDS, ANNOTATION_SCHEMA, Annotation
from .fs import LocalFileSystem, RemoteMissingDepsError, Schemes, get_cloud_fs
from .fs.callbacks import DEFAULT_CALLBACK, Callback, TqdmCallback
from .utils import relpath
from .utils.fs import path_isin

if TYPE_CHECKING:
    from dvc_data.hashfile.obj import HashFile
    from dvc_data.index import DataIndexKey

    from .ignore import DvcIgnoreFilter

logger = logger.getChild(__name__)


CHECKSUM_SCHEMA = vol.Any(
    None,
    vol.And(str, vol.Length(max=0), vol.SetTo(None)),
    vol.And(vol.Any(str, vol.And(int, vol.Coerce(str))), vol.Length(min=3), vol.Lower),
)

CASE_SENSITIVE_CHECKSUM_SCHEMA = vol.Any(
    None,
    vol.And(str, vol.Length(max=0), vol.SetTo(None)),
    vol.And(vol.Any(str, vol.And(int, vol.Coerce(str))), vol.Length(min=3)),
)

# NOTE: currently there are only 3 possible checksum names:
#
#    1) md5 (LOCAL, SSH) (actually DVC 2.x md5-dos2unix)
#    2) etag (S3, GS, OSS, AZURE, HTTP);
#    3) checksum (HDFS);
#
# so when a few types of outputs share the same name, we only need
# specify it once.
HDFS_PARAM_CHECKSUM = "checksum"
S3_PARAM_CHECKSUM = "etag"
CHECKSUMS_SCHEMA = {
    "md5": CHECKSUM_SCHEMA,  # DVC 2.x md5-dos2unix
    HDFS_PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    S3_PARAM_CHECKSUM: CASE_SENSITIVE_CHECKSUM_SCHEMA,
}


def _get(stage, path, **kwargs):
    return Output(stage, path, **kwargs)


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(Output.PARAM_PATH)
        cache = d.pop(Output.PARAM_CACHE, True)
        metric = d.pop(Output.PARAM_METRIC, False)
        plot = d.pop(Output.PARAM_PLOT, False)
        persist = d.pop(Output.PARAM_PERSIST, False)
        remote = d.pop(Output.PARAM_REMOTE, None)
        annot = {field: d.pop(field, None) for field in ANNOTATION_FIELDS}
        files = d.pop(Output.PARAM_FILES, None)
        push = d.pop(Output.PARAM_PUSH, True)
        hash_name = d.pop(Output.PARAM_HASH, None)
        fs_config = d.pop(Output.PARAM_FS_CONFIG, None)
        ret.append(
            _get(
                stage,
                p,
                info=d,
                cache=cache,
                metric=metric,
                plot=plot,
                persist=persist,
                remote=remote,
                **annot,
                files=files,
                push=push,
                hash_name=hash_name,
                fs_config=fs_config,
            )
        )
    return ret


def loads_from(
    stage,
    s_list,
    use_cache=True,
    metric=False,
    plot=False,
    persist=False,
    remote=None,
    push=True,
):
    return [
        _get(
            stage,
            s,
            info={},
            cache=use_cache,
            metric=metric,
            plot=plot,
            persist=persist,
            remote=remote,
            push=push,
        )
        for s in s_list
    ]


def _split_dict(d, keys):
    return project(d, keys), project(d, d.keys() - keys)


def _merge_data(s_list):
    d: dict[str, dict] = defaultdict(dict)
    for key in s_list:
        if isinstance(key, str):
            d[key].update({})
            continue
        if not isinstance(key, dict):
            raise ValueError(f"'{type(key).__name__}' not supported.")  # noqa: TRY004

        for k, flags in key.items():
            if not isinstance(flags, dict):
                raise ValueError(  # noqa: TRY004
                    f"Expected dict for '{k}', got: '{type(flags).__name__}'"
                )
            d[k].update(flags)
    return d


@collecting
def load_from_pipeline(stage, data, typ="outs"):
    if typ not in (stage.PARAM_OUTS, stage.PARAM_METRICS, stage.PARAM_PLOTS):
        raise ValueError(f"'{typ}' key is not allowed for pipeline files.")

    metric = typ == stage.PARAM_METRICS
    plot = typ == stage.PARAM_PLOTS

    d = _merge_data(data)

    for path, flags in d.items():
        plt_d = {}
        if plot:
            from dvc.schema import PLOT_PROPS

            plt_d, flags = _split_dict(flags, keys=PLOT_PROPS.keys())

        extra = project(
            flags,
            [
                Output.PARAM_CACHE,
                Output.PARAM_PERSIST,
                Output.PARAM_REMOTE,
                Output.PARAM_PUSH,
                *ANNOTATION_FIELDS,
            ],
        )

        yield _get(stage, path, info={}, plot=plt_d or plot, metric=metric, **extra)


def split_file_meta_from_cloud(entry: dict) -> dict:
    if remote_name := entry.pop(Meta.PARAM_REMOTE, None):
        remote_meta = {}
        for key in (S3_PARAM_CHECKSUM, HDFS_PARAM_CHECKSUM, Meta.PARAM_VERSION_ID):
            if value := entry.pop(key, None):
                remote_meta[key] = value

        if remote_meta:
            entry[Output.PARAM_CLOUD] = {remote_name: remote_meta}
    return entry


def merge_file_meta_from_cloud(entry: dict) -> dict:
    cloud_meta = entry.pop(Output.PARAM_CLOUD, {})
    if remote_name := first(cloud_meta):
        entry.update(cloud_meta[remote_name])
        entry[Meta.PARAM_REMOTE] = remote_name
    return entry


def _serialize_tree_obj_to_files(obj: Tree) -> list[dict[str, Any]]:
    key = obj.PARAM_RELPATH
    return sorted(
        (
            {
                key: posixpath.sep.join(parts),
                **_serialize_hi_to_dict(hi),
                **meta.to_dict(),
            }
            for parts, meta, hi in obj
        ),
        key=itemgetter(key),
    )


def _serialize_hi_to_dict(hash_info: Optional[HashInfo]) -> dict[str, Any]:
    if hash_info:
        if hash_info.name == "md5-dos2unix":
            return {"md5": hash_info.value}
        return hash_info.to_dict()
    return {}


class OutputDoesNotExistError(DvcException):
    def __init__(self, path):
        msg = f"output '{path}' does not exist"
        super().__init__(msg)


class OutputIsNotFileOrDirError(DvcException):
    def __init__(self, path):
        msg = f"output '{path}' is not a file or directory"
        super().__init__(msg)


class OutputAlreadyTrackedError(DvcException):
    def __init__(self, path):
        msg = f""" output '{path}' is already tracked by SCM (e.g. Git).
    You can remove it from Git, then add to DVC.
        To stop tracking from Git:
            git rm -r --cached '{path}'
            git commit -m "stop tracking {path}" """
        super().__init__(msg)


class OutputIsStageFileError(DvcException):
    def __init__(self, path):
        super().__init__(f"DVC file '{path}' cannot be an output.")


class OutputIsIgnoredError(DvcException):
    def __init__(self, match):
        lines = "\n".join(match.patterns)
        super().__init__(f"Path '{match.file}' is ignored by\n{lines}")


class CheckoutCallback(TqdmCallback):
    # disable branching for checkouts
    branch = Callback.branch  # type: ignore[assignment]


class Output:
    IS_DEPENDENCY = False

    PARAM_PATH = "path"
    PARAM_CACHE = "cache"
    PARAM_FILES = "files"
    PARAM_METRIC = "metric"
    PARAM_METRIC_TYPE = "type"
    PARAM_METRIC_XPATH = "xpath"
    PARAM_PLOT = "plot"
    PARAM_PLOT_TEMPLATE = "template"
    PARAM_PLOT_X = "x"
    PARAM_PLOT_Y = "y"
    PARAM_PLOT_X_LABEL = "x_label"
    PARAM_PLOT_Y_LABEL = "y_label"
    PARAM_PLOT_TITLE = "title"
    PARAM_PLOT_HEADER = "header"
    PARAM_PERSIST = "persist"
    PARAM_REMOTE = "remote"
    PARAM_PUSH = "push"
    PARAM_CLOUD = "cloud"
    PARAM_HASH = "hash"
    PARAM_FS_CONFIG = "fs_config"

    DoesNotExistError: type[DvcException] = OutputDoesNotExistError
    IsNotFileOrDirError: type[DvcException] = OutputIsNotFileOrDirError
    IsStageFileError: type[DvcException] = OutputIsStageFileError
    IsIgnoredError: type[DvcException] = OutputIsIgnoredError

    def __init__(  # noqa: PLR0913
        self,
        stage,
        path,
        info=None,
        cache=True,
        metric=False,
        plot=False,
        persist=False,
        desc=None,
        type=None,  # noqa: A002
        labels=None,
        meta=None,
        remote=None,
        repo=None,
        fs_config=None,
        files: Optional[list[dict[str, Any]]] = None,
        push: bool = True,
        hash_name: Optional[str] = DEFAULT_ALGORITHM,
    ):
        self.annot = Annotation(
            desc=desc, type=type, labels=labels or [], meta=meta or {}
        )
        self.repo = stage.repo if not repo and stage else repo
        meta_d = merge_file_meta_from_cloud(info or {})
        meta = Meta.from_dict(meta_d)
        # NOTE: when version_aware is not passed into get_cloud_fs, it will be
        # set based on whether or not path is versioned
        fs_kwargs = {}
        if meta.version_id or files:
            fs_kwargs["version_aware"] = True

        self.def_fs_config = fs_config
        if fs_config is not None:
            fs_kwargs.update(**fs_config)

        fs_cls, fs_config, fs_path = get_cloud_fs(
            self.repo.config if self.repo else {},
            url=path,
            **fs_kwargs,
        )
        self.fs = fs_cls(**fs_config)

        if (
            self.fs.protocol == "local"
            and stage
            and isinstance(stage.repo.fs, LocalFileSystem)
            and path_isin(path, stage.repo.root_dir)
        ):
            self.def_path: str = relpath(path, stage.wdir)
            self.fs = stage.repo.fs
        else:
            self.def_path = path

        if (
            self.repo
            and self.fs.protocol == "local"
            and not self.fs.isabs(self.def_path)
        ):
            self.fs = self.repo.fs

        self._validate_output_path(path, stage)
        # This output (and dependency) objects have too many paths/urls
        # here is a list and comments:
        #
        #   .def_path - path from definition in DVC file
        #   .fspath - local only, resolved
        #   .__str__ - for presentation purposes, def_path/relpath
        #
        # By resolved path, which contains actual location,
        # should be absolute and don't contain remote:// refs.
        self.stage = stage
        self.meta = meta

        if files is not None:
            files = [merge_file_meta_from_cloud(f) for f in files]
        self.files = files
        self.use_cache = False if self.IS_DEPENDENCY else cache
        self.metric = False if self.IS_DEPENDENCY else metric
        self.plot = False if self.IS_DEPENDENCY else plot
        self.persist = persist
        self.can_push = push

        self.fs_path = self._parse_path(self.fs, fs_path)
        self.obj: Optional[HashFile] = None

        self.remote = remote

        if self.fs.version_aware:
            _, version_id = self.fs.coalesce_version(
                self.def_path, self.meta.version_id
            )
            self.meta.version_id = version_id

        self.hash_name, self.hash_info = self._compute_hash_info_from_meta(hash_name)
        self._compute_meta_hash_info_from_files()

    def _compute_hash_info_from_meta(
        self, hash_name: Optional[str]
    ) -> tuple[str, HashInfo]:
        if self.is_in_repo:
            if hash_name is None:
                # Legacy 2.x output, use "md5-dos2unix" but read "md5" from
                # file meta
                hash_name = "md5-dos2unix"
                meta_name = "md5"
            else:
                meta_name = hash_name
        else:
            hash_name = meta_name = self.fs.PARAM_CHECKSUM
        assert hash_name

        hash_info = HashInfo(name=hash_name, value=getattr(self.meta, meta_name, None))
        return hash_name, hash_info

    def _compute_meta_hash_info_from_files(self) -> None:
        if self.files:
            tree = Tree.from_list(self.files, hash_name=self.hash_name)
            tree.digest(with_meta=True)

            self.hash_info = tree.hash_info
            self.meta.isdir = True
            self.meta.nfiles = len(self.files)
            self.meta.size = sum(filter(None, (f.get("size") for f in self.files)))
            self.meta.remote = first(f.get("remote") for f in self.files)
        elif self.meta.nfiles or self.hash_info and self.hash_info.isdir:
            self.meta.isdir = True
            if not self.hash_info and self.hash_name not in ("md5", "md5-dos2unix"):
                md5 = getattr(self.meta, "md5", None)
                if md5:
                    self.hash_info = HashInfo("md5", md5)

    def _parse_path(self, fs, fs_path):
        parsed = urlparse(self.def_path)
        if (
            parsed.scheme != "remote"
            and self.stage
            and self.stage.repo.fs == fs
            and not fs.isabs(fs_path)
        ):
            # NOTE: we can path either from command line or .dvc file,
            # so we should expect both posix and windows style paths.
            # paths accepts both, i.e. / works everywhere, \ only on win.
            #
            # FIXME: if we have Windows path containing / or posix one with \
            # then we have #2059 bug and can't really handle that.
            fs_path = fs.join(self.stage.wdir, fs_path)

        return fs.abspath(fs.normpath(fs_path))

    def __repr__(self):
        return f"{type(self).__name__}: {self.def_path!r}"

    def __str__(self):
        if self.fs.protocol != "local":
            return self.def_path

        if (
            not self.repo
            or urlparse(self.def_path).scheme == "remote"
            or os.path.isabs(self.def_path)
        ):
            return str(self.def_path)

        if not self.fs.isin(self.fs_path, self.repo.root_dir):
            return self.fs_path

        cur_dir = self.fs.getcwd()
        if self.fs.isin(cur_dir, self.repo.root_dir):
            return self.fs.relpath(self.fs_path, cur_dir)

        return self.fs.relpath(self.fs_path, self.repo.root_dir)

    def clear(self):
        self.hash_info = HashInfo.from_dict({})
        self.meta = Meta.from_dict({})
        self.obj = None
        self.files = None

    @property
    def protocol(self):
        return self.fs.protocol

    @property
    def is_in_repo(self):
        if urlparse(self.def_path).scheme == "remote":
            return False

        if self.fs.isabs(self.def_path):
            return False

        return self.repo and self.fs.isin(self.fs_path, self.repo.root_dir)

    @property
    def use_scm_ignore(self):
        if not self.is_in_repo:
            return False

        return self.use_cache or self.stage.is_repo_import

    @property
    def cache(self):
        from dvc.cachemgr import LEGACY_HASH_NAMES

        assert self.is_in_repo
        odb_name = "legacy" if self.hash_name in LEGACY_HASH_NAMES else "repo"
        return getattr(self.repo.cache, odb_name)

    @property
    def local_cache(self):
        from dvc.cachemgr import LEGACY_HASH_NAMES

        if self.hash_name in LEGACY_HASH_NAMES:
            return self.repo.cache.legacy
        return self.repo.cache.local

    @property
    def cache_path(self):
        return self.cache.fs.unstrip_protocol(
            self.cache.oid_to_path(self.hash_info.value)
        )

    def get_hash(self):
        _, hash_info = self._get_hash_meta()
        return hash_info

    def _build(
        self, *args, no_progress_bar=False, **kwargs
    ) -> tuple["HashFileDB", "Meta", "HashFile"]:
        from dvc.ui import ui

        with ui.progress(
            unit="file",
            desc=f"Collecting files and computing hashes in {self}",
            disable=no_progress_bar,
        ) as pb:
            return build(*args, callback=pb.as_callback(), **kwargs)

    def _get_hash_meta(self):
        if self.use_cache:
            odb = self.cache
        else:
            odb = self.local_cache
        _, meta, obj = self._build(
            odb,
            self.fs_path,
            self.fs,
            self.hash_name,
            ignore=self.dvcignore,
            dry_run=not self.use_cache,
        )
        return meta, obj.hash_info

    def get_meta(self) -> Meta:
        meta, _ = self._get_hash_meta()
        return meta

    @property
    def is_dir_checksum(self):
        return self.hash_info.isdir

    def _is_path_dvcignore(self, path) -> bool:
        if self.IS_DEPENDENCY or not self.dvcignore:
            return False
        return self.dvcignore.is_ignored(self.fs, path, ignore_subrepos=False)

    @property
    def exists(self):
        if self._is_path_dvcignore(self.fs_path):
            return False

        return self.fs.exists(self.fs_path)

    @cached_property
    def index_key(self) -> tuple[str, "DataIndexKey"]:
        if self.is_in_repo:
            workspace = "repo"
            key = self.repo.fs.relparts(self.fs_path, self.repo.root_dir)
        else:
            workspace = self.fs.protocol
            no_drive = self.fs.flavour.splitdrive(self.fs_path)[1]
            key = self.fs.parts(no_drive)[1:]
        return workspace, key

    def changed_checksum(self):
        return self.hash_info != self.get_hash()

    def changed_cache(self, filter_info=None):
        if not self.use_cache or not self.hash_info:
            return True

        obj = self.get_obj(filter_info=filter_info)
        if not obj:
            return True

        try:
            ocheck(self.cache, obj)
            return False
        except (FileNotFoundError, ObjectFormatError):
            return True

    def changed_meta(self) -> bool:
        if self.fs.version_aware and self.meta.version_id:
            return self.meta.version_id != self.get_meta().version_id
        return False

    def workspace_status(self) -> dict[str, str]:
        if not self.exists:
            return {str(self): "deleted"}

        if self.changed_checksum():
            return {str(self): "modified"}

        if not self.hash_info:
            return {str(self): "new"}

        return {}

    def status(self) -> dict[str, str]:
        if self.hash_info and self.use_cache and self.changed_cache():
            return {str(self): "not in cache"}

        return self.workspace_status()

    def changed(self) -> bool:
        status = self.status()
        logger.debug(str(status))
        return bool(status)

    @property
    def dvcignore(self) -> Optional["DvcIgnoreFilter"]:
        if self.fs.protocol == "local":
            return self.repo.dvcignore
        return None

    @property
    def is_empty(self) -> bool:
        return self.fs.is_empty(self.fs_path)

    def isdir(self) -> bool:
        if self._is_path_dvcignore(self.fs_path):
            return False
        return self.fs.isdir(self.fs_path)

    def isfile(self) -> bool:
        if self._is_path_dvcignore(self.fs_path):
            return False
        return self.fs.isfile(self.fs_path)

    def ignore(self) -> None:
        if not self.use_scm_ignore:
            return

        if self.repo.scm.is_tracked(self.fspath):
            raise OutputAlreadyTrackedError(self)

        self.repo.scm_context.ignore(self.fspath)

    def ignore_remove(self) -> None:
        if not self.use_scm_ignore:
            return

        self.repo.scm_context.ignore_remove(self.fspath)

    def save(self) -> None:
        if self.use_cache and not self.is_in_repo:
            raise DvcException(
                f"Saving cached external output {self!s} is not supported "
                "since DVC 3.0. See "
                f"{format_link('https://dvc.org/doc/user-guide/upgrade')} "
                "for more info."
            )

        if not self.exists:
            raise self.DoesNotExistError(self)

        if not self.isfile() and not self.isdir():
            raise self.IsNotFileOrDirError(self)

        if self.is_empty:
            logger.warning("'%s' is empty.", self)

        self.ignore()

        if self.metric:
            self.verify_metric()

        self.update_legacy_hash_name()
        if self.use_cache:
            _, self.meta, self.obj = self._build(
                self.cache,
                self.fs_path,
                self.fs,
                self.hash_name,
                ignore=self.dvcignore,
            )
        else:
            _, self.meta, self.obj = self._build(
                self.local_cache,
                self.fs_path,
                self.fs,
                self.hash_name,
                ignore=self.dvcignore,
                dry_run=True,
            )
            if not self.IS_DEPENDENCY:
                logger.debug("Output '%s' doesn't use cache. Skipping saving.", self)

        self.hash_info = self.obj.hash_info
        self.files = None

    def update_legacy_hash_name(self, force: bool = False):
        if self.hash_name == "md5-dos2unix" and (force or self.changed_checksum()):
            self.hash_name = "md5"

    def set_exec(self) -> None:
        if self.isfile() and self.meta.isexec:
            self.cache.set_exec(self.fs_path)

    def _checkout(self, *args, **kwargs) -> Optional[bool]:
        from dvc_data.hashfile.checkout import CheckoutError as _CheckoutError
        from dvc_data.hashfile.checkout import LinkError, PromptError

        kwargs.setdefault("ignore", self.dvcignore)
        try:
            return checkout(*args, **kwargs)
        except PromptError as exc:
            raise ConfirmRemoveError(exc.path)  # noqa: B904
        except LinkError as exc:
            raise CacheLinkError([exc.path])  # noqa: B904
        except _CheckoutError as exc:
            raise CheckoutError(exc.paths, {})  # noqa: B904

    def commit(self, filter_info=None, relink=True) -> None:
        if not self.exists:
            raise self.DoesNotExistError(self)

        assert self.hash_info

        if self.use_cache:
            granular = (
                self.is_dir_checksum and filter_info and filter_info != self.fs_path
            )
            # NOTE: trying to use hardlink during transfer only if we will be
            # relinking later
            hardlink = relink
            if granular:
                obj = self._commit_granular_dir(filter_info, hardlink)
            else:
                staging, _, obj = self._build(
                    self.cache,
                    filter_info or self.fs_path,
                    self.fs,
                    self.hash_name,
                    ignore=self.dvcignore,
                )
                with TqdmCallback(
                    desc=f"Committing {self} to cache",
                    unit="file",
                ) as cb:
                    otransfer(
                        staging,
                        self.cache,
                        {obj.hash_info},
                        shallow=False,
                        hardlink=hardlink,
                        callback=cb,
                    )
            if relink:
                rel = self.fs.relpath(filter_info or self.fs_path)
                with CheckoutCallback(desc=f"Checking out {rel}", unit="files") as cb:
                    self._checkout(
                        filter_info or self.fs_path,
                        self.fs,
                        obj,
                        self.cache,
                        relink=True,
                        state=self.repo.state,
                        prompt=prompt.confirm,
                        progress_callback=cb,
                    )
                self.set_exec()

    def _commit_granular_dir(self, filter_info, hardlink) -> Optional["HashFile"]:
        prefix = self.fs.parts(self.fs.relpath(filter_info, self.fs_path))
        staging, _, obj = self._build(
            self.cache, self.fs_path, self.fs, self.hash_name, ignore=self.dvcignore
        )
        assert isinstance(obj, Tree)
        save_obj = obj.filter(prefix)
        assert isinstance(save_obj, Tree)
        checkout_obj = save_obj.get_obj(self.cache, prefix)
        with TqdmCallback(desc=f"Committing {self} to cache", unit="file") as cb:
            otransfer(
                staging,
                self.cache,
                {save_obj.hash_info} | {oid for _, _, oid in save_obj},
                shallow=True,
                hardlink=hardlink,
                callback=cb,
            )
        return checkout_obj

    def dumpd(self, **kwargs):  # noqa: C901, PLR0912
        from dvc.cachemgr import LEGACY_HASH_NAMES

        ret: dict[str, Any] = {}
        with_files = (
            (not self.IS_DEPENDENCY or kwargs.get("datasets") or self.stage.is_import)
            and self.hash_info.isdir
            and (kwargs.get("with_files") or self.files is not None)
        )

        if not with_files:
            meta_d = self.meta.to_dict()
            meta_d.pop("isdir", None)
            if self.hash_name in LEGACY_HASH_NAMES:
                # 2.x checksums get serialized with file meta
                name = "md5" if self.hash_name == "md5-dos2unix" else self.hash_name
                ret.update({name: self.hash_info.value})
            else:
                ret.update(self.hash_info.to_dict())
            ret.update(split_file_meta_from_cloud(meta_d))

        if self.is_in_repo:
            path = self.fs.as_posix(relpath(self.fs_path, self.stage.wdir))
        else:
            path = self.def_path

        if self.hash_name not in LEGACY_HASH_NAMES:
            ret[self.PARAM_HASH] = "md5"

        ret[self.PARAM_PATH] = path

        if self.def_fs_config:
            ret[self.PARAM_FS_CONFIG] = self.def_fs_config

        if not self.IS_DEPENDENCY:
            ret.update(self.annot.to_dict())
            if not self.use_cache:
                ret[self.PARAM_CACHE] = self.use_cache

            if (
                isinstance(self.metric, dict)
                and self.PARAM_METRIC_XPATH in self.metric
                and not self.metric[self.PARAM_METRIC_XPATH]
            ):
                del self.metric[self.PARAM_METRIC_XPATH]

            if self.metric:
                ret[self.PARAM_METRIC] = self.metric

            if self.plot:
                ret[self.PARAM_PLOT] = self.plot

            if self.persist:
                ret[self.PARAM_PERSIST] = self.persist

            if self.remote:
                ret[self.PARAM_REMOTE] = self.remote

            if not self.can_push:
                ret[self.PARAM_PUSH] = self.can_push

        if with_files:
            obj = self.obj or self.get_obj()
            if obj:
                assert isinstance(obj, Tree)
                ret[self.PARAM_FILES] = [
                    split_file_meta_from_cloud(f)
                    for f in _serialize_tree_obj_to_files(obj)
                ]
        return ret

    def verify_metric(self):
        if self.fs.protocol != "local":
            raise DvcException(f"verify metric is not supported for {self.protocol}")
        if not self.metric:
            return

        if not os.path.exists(self.fs_path):
            return

        if os.path.isdir(self.fs_path):
            msg = "directory '%s' cannot be used as %s."
            logger.debug(msg, str(self), "metrics")
            return

        if not istextfile(self.fs_path, self.fs):
            raise DvcException(
                f"binary file '{self.fs_path}' cannot be used as metrics."
            )

    def get_obj(
        self, filter_info: Optional[str] = None, **kwargs
    ) -> Optional["HashFile"]:
        obj: Optional[HashFile] = None
        if self.obj:
            obj = self.obj
        elif self.files:
            tree = Tree.from_list(self.files, hash_name=self.hash_name)
            tree.digest()
            obj = tree
        elif self.hash_info:
            try:
                obj = oload(self.cache, self.hash_info)
            except (FileNotFoundError, ObjectFormatError):
                return None
        else:
            return None

        assert obj
        fs_path = self.fs
        if filter_info and filter_info != self.fs_path:
            prefix = fs_path.relparts(filter_info, self.fs_path)
            assert isinstance(obj, Tree)
            obj = obj.get_obj(self.cache, prefix)

        return obj

    def checkout(
        self,
        force: bool = False,
        progress_callback: "Callback" = DEFAULT_CALLBACK,
        relink: bool = False,
        filter_info: Optional[str] = None,
        allow_missing: bool = False,
        **kwargs,
    ) -> Optional[tuple[bool, Optional[bool]]]:
        # callback passed act as a aggregate callback.
        # do not let checkout to call set_size and change progressbar.
        class CallbackProxy(Callback):
            def relative_update(self, inc: int = 1) -> None:
                progress_callback.relative_update(inc)
                return super().relative_update(inc)

        callback = CallbackProxy()
        if not self.use_cache:
            callback.relative_update(self.get_files_number(filter_info))
            return None

        obj = self.get_obj(filter_info=filter_info)
        if not obj and (filter_info and filter_info != self.fs_path):
            # backward compatibility
            return None

        added = not self.exists

        try:
            modified = self._checkout(
                filter_info or self.fs_path,
                self.fs,
                obj,
                self.cache,
                force=force,
                progress_callback=callback,
                relink=relink,
                state=self.repo.state,
                prompt=prompt.confirm,
                **kwargs,
            )
        except CheckoutError:
            if allow_missing:
                return None
            raise
        self.set_exec()
        return added, False if added else modified

    def remove(self, ignore_remove=False):
        try:
            self.fs.remove(self.fs_path, recursive=True)
        except FileNotFoundError:
            pass
        if self.protocol != Schemes.LOCAL:
            return

        if ignore_remove:
            self.ignore_remove()

    def move(self, out):
        if self.protocol == "local" and self.use_scm_ignore:
            self.repo.scm_context.ignore_remove(self.fspath)

        self.fs.move(self.fs_path, out.fs_path)
        self.def_path = out.def_path
        self.fs_path = out.fs_path
        self.save()
        self.commit()

        if self.protocol == "local" and self.use_scm_ignore:
            self.repo.scm_context.ignore(self.fspath)

    def transfer(
        self, source, odb=None, jobs=None, update=False, no_progress_bar=False
    ):
        if odb is None:
            odb = self.cache

        cls, config, from_info = get_cloud_fs(
            self.repo.config if self.repo else {}, url=source
        )
        from_fs = cls(**config)

        # When running import-url --to-remote / add --to-remote/-o ... we
        # assume that it is unlikely that the odb will contain majority of the
        # hashes, so we transfer everything as is (even if that file might
        # already be in the cache) and don't waste an upload to scan the layout
        # of the source location. But when doing update --to-remote, there is
        # a high probability that the odb might contain some of the hashes, so
        # we first calculate all the hashes (but don't transfer anything) and
        # then only update the missing cache files.

        upload = not (update and from_fs.isdir(from_info))
        jobs = jobs or min((from_fs.jobs, odb.fs.jobs))
        staging, self.meta, obj = self._build(
            odb,
            from_info,
            from_fs,
            DEFAULT_ALGORITHM,
            upload=upload,
            no_progress_bar=no_progress_bar,
        )
        with TqdmCallback(
            desc=f"Transferring to {odb.fs.unstrip_protocol(odb.path)}",
            unit="file",
        ) as cb:
            otransfer(
                staging,
                odb,
                {obj.hash_info},
                jobs=jobs,
                hardlink=False,
                shallow=False,
                callback=cb,
            )

        self.hash_info = obj.hash_info
        self.files = None
        return obj

    def get_files_number(self, filter_info=None):
        if not self.use_cache or not self.hash_info:
            return 0

        if not self.hash_info.isdir:
            return 1

        if not filter_info or filter_info == self.fs_path:
            return self.meta.nfiles or 0

        obj = self.get_obj(filter_info=filter_info)
        return len(obj) if obj else 0

    def unprotect(self):
        if self.exists and self.use_cache:
            with TqdmCallback(
                size=self.meta.nfiles or -1, desc=f"Unprotecting {self}"
            ) as callback:
                self.cache.unprotect(self.fs_path, callback=callback)

    def get_dir_cache(self, **kwargs) -> Optional["Tree"]:
        if not self.is_dir_checksum:
            raise DvcException("cannot get dir cache for file checksum")

        obj = self.cache.get(self.hash_info.value)
        try:
            ocheck(self.cache, obj)
        except FileNotFoundError:
            if self.remote:
                kwargs["remote"] = self.remote
            with suppress(Exception):
                self.repo.cloud.pull([obj.hash_info], **kwargs)

        if self.obj:
            assert isinstance(self.obj, Tree)
            return self.obj

        try:
            obj = oload(self.cache, self.hash_info)
            assert isinstance(obj, Tree)
        except (FileNotFoundError, ObjectFormatError):
            obj = None

        self.obj = obj
        return obj

    def _collect_used_dir_cache(
        self, remote=None, force=False, jobs=None, filter_info=None
    ) -> Optional["Tree"]:
        """Fetch dir cache and return used object IDs for this out."""

        try:
            self.get_dir_cache(jobs=jobs, remote=remote)
        except RemoteMissingDepsError:
            raise
        except DvcException:
            logger.debug("failed to pull cache for '%s'", self)

        try:
            ocheck(self.cache, self.cache.get(self.hash_info.value))
        except FileNotFoundError:
            msg = (
                "Missing cache for directory '{}'. "
                "Cache for files inside will be lost. "
                "Would you like to continue? Use '-f' to force."
            )
            if not force and not prompt.confirm(msg.format(self.fs_path)):
                raise CollectCacheError(  # noqa: B904
                    "unable to fully collect used cache"
                    f" without cache for directory '{self}'"
                )
            return None

        obj = self.get_obj()
        assert obj is None or isinstance(obj, Tree)
        if filter_info and filter_info != self.fs_path:
            assert obj
            prefix = self.fs.parts(self.fs.relpath(filter_info, self.fs_path))
            return obj.filter(prefix)
        return obj

    def get_used_objs(  # noqa: PLR0911
        self, **kwargs
    ) -> dict[Optional["HashFileDB"], set["HashInfo"]]:
        """Return filtered set of used object IDs for this out."""
        from dvc.cachemgr import LEGACY_HASH_NAMES

        if not self.use_cache:
            return {}

        push: bool = kwargs.pop("push", False)
        if self.stage.is_repo_import:
            return {}

        if push and not self.can_push:
            return {}

        if not self.hash_info:
            msg = (
                f"Output '{self}'({self.stage}) is missing version info. "
                "Cache for it will not be collected. "
                "Use `dvc repro` to get your pipeline up to date."
            )
            if self.exists:
                msg += (
                    "\n"
                    f"You can also use `dvc commit {self.stage.addressing}` "
                    f"to associate existing '{self}' with {self.stage}."
                )
            logger.warning(msg)
            return {}

        obj: Optional[HashFile]
        if self.is_dir_checksum:
            obj = self._collect_used_dir_cache(**kwargs)
        else:
            obj = self.get_obj(filter_info=kwargs.get("filter_info"))
            if not obj:
                obj = self.cache.get(self.hash_info.value)

        if not obj:
            return {}

        if self.remote:
            remote_odb = self.repo.cloud.get_remote_odb(
                name=self.remote, hash_name=self.hash_name
            )
            other_odb = self.repo.cloud.get_remote_odb(
                name=self.remote,
                hash_name=(
                    "md5" if self.hash_name in LEGACY_HASH_NAMES else "md5-dos2unix"
                ),
            )
            return {remote_odb: self._named_obj_ids(obj), other_odb: set()}
        return {None: self._named_obj_ids(obj)}

    def _named_obj_ids(self, obj):
        name = str(self)
        obj.hash_info.obj_name = name
        oids = {obj.hash_info}
        if isinstance(obj, Tree):
            for key, _, oid in obj:
                oid.obj_name = self.fs.sep.join([name, *key])
                oids.add(oid)
        return oids

    def _validate_output_path(self, path, stage=None):
        from dvc.dvcfile import is_valid_filename

        if is_valid_filename(path):
            raise self.IsStageFileError(path)

        if stage:
            abs_path = os.path.join(stage.wdir, path)
            if self._is_path_dvcignore(abs_path):
                check = stage.repo.dvcignore.check_ignore(abs_path)
                raise self.IsIgnoredError(check)

    def _check_can_merge(self, out):
        if self.protocol != out.protocol:
            raise MergeError("unable to auto-merge outputs of different types")

        my = self.dumpd()
        other = out.dumpd()

        ignored = [
            self.hash_name,
            Meta.PARAM_SIZE,
            Meta.PARAM_NFILES,
            Output.PARAM_HASH,
        ]

        for opt in ignored:
            my.pop(opt, None)
            other.pop(opt, None)

        if my != other or self.hash_name != out.hash_name:
            raise MergeError("unable to auto-merge outputs with different options")

        if not out.is_dir_checksum:
            raise MergeError("unable to auto-merge outputs that are not directories")

    def merge(self, ancestor, other, allowed=None):
        from dvc_data.hashfile.tree import MergeError as TreeMergeError
        from dvc_data.hashfile.tree import merge

        assert other

        if ancestor:
            self._check_can_merge(ancestor)
            ancestor_info = ancestor.hash_info
        else:
            ancestor_info = None

        self._check_can_merge(self)
        self._check_can_merge(other)

        try:
            merged = merge(
                self.cache,
                ancestor_info,
                self.hash_info,
                other.hash_info,
                allowed=allowed,
            )
        except TreeMergeError as exc:
            raise MergeError(str(exc)) from exc

        self.cache.add(merged.path, merged.fs, merged.oid)

        self.hash_info = merged.hash_info
        self.files = None
        self.meta = Meta(size=du(self.cache, merged), nfiles=len(merged))

    def unstage(self, path: str) -> tuple["Meta", "Tree"]:
        from pygtrie import Trie

        rel_key = tuple(self.fs.parts(self.fs.relpath(path, self.fs_path)))

        if self.hash_info:
            tree = self.get_dir_cache()
            if tree is None:
                raise DvcException(f"could not read {self.hash_info.value!r}")
        else:
            tree = Tree()

        trie = tree.as_trie()
        assert isinstance(trie, Trie)

        try:
            del trie[rel_key:]  # type: ignore[misc]
        except KeyError:
            raise FileNotFoundError(  # noqa: B904
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                path,
            )

        new = tree.from_trie(trie)
        new.digest()
        return Meta(nfiles=len(new), isdir=True), new

    def apply(
        self,
        path: str,
        obj: Union["Tree", "HashFile"],
        meta: "Meta",
    ) -> tuple["Meta", "Tree"]:
        from pygtrie import Trie

        append_only = True
        rel_key = tuple(self.fs.parts(self.fs.relpath(path, self.fs_path)))

        if self.hash_info:
            tree = self.get_dir_cache()
            if tree is None:
                raise DvcException(f"could not read {self.hash_info.value!r}")
        else:
            tree = Tree()

        trie = tree.as_trie()
        assert isinstance(trie, Trie)

        try:
            del trie[rel_key:]  # type: ignore[misc]
        except KeyError:
            pass
        else:
            append_only = False

        items = {}
        if isinstance(obj, Tree):
            items = {(*rel_key, *key): (m, o) for key, m, o in obj}
        else:
            items = {rel_key: (meta, obj.hash_info)}
        trie.update(items)

        new = Tree.from_trie(trie)
        new.digest()

        size = self.meta.size if self.meta and self.meta.size else None
        if append_only and size and meta.size is not None:
            # if files were only appended, we can sum to the existing size
            size += meta.size
        elif self.hash_info and self.hash_info == new.hash_info:
            # if hashes are same, sizes must have been the same
            size = self.meta.size
        else:
            size = None

        meta = Meta(nfiles=len(new), size=size, isdir=True)
        return meta, new

    def add(  # noqa: C901
        self, path: Optional[str] = None, no_commit: bool = False, relink: bool = True
    ) -> Optional["HashFile"]:
        path = path or self.fs_path
        if self.hash_info and not self.is_dir_checksum and self.fs_path != path:
            raise DvcException(
                f"Cannot modify '{self}' which is being tracked as a file"
            )

        assert self.repo
        self.update_legacy_hash_name()
        cache = self.cache if self.use_cache else self.local_cache
        assert isinstance(cache, HashFileDB)

        new: HashFile
        try:
            assert self.hash_name
            staging, meta, obj = self._build(
                cache,
                path,
                self.fs,
                self.hash_name,
                ignore=self.dvcignore,
                dry_run=not self.use_cache,
            )
        except FileNotFoundError as exc:
            if not self.exists:
                raise self.DoesNotExistError(self) from exc
            if not self.is_dir_checksum:
                raise

            meta, new = self.unstage(path)
            staging, obj = None, None
        else:
            assert obj
            assert staging
            if self.fs_path != path:
                meta, new = self.apply(path, obj, meta)
                add_update_tree(staging, new)
            else:
                new = obj

        self.obj = new
        self.hash_info = self.obj.hash_info
        self.meta = meta
        self.files = None
        self.ignore()

        if no_commit or not self.use_cache:
            return obj

        if isinstance(new, Tree):
            add_update_tree(cache, new)

        if not obj:
            return obj

        assert staging
        assert obj.hash_info
        with TqdmCallback(desc=f"Adding {self} to cache", unit="file") as cb:
            otransfer(
                staging,
                self.cache,
                {obj.hash_info},
                hardlink=relink,
                shallow=False,
                callback=cb,
            )

        if relink:
            with CheckoutCallback(
                desc=f"Checking out {path}", unit="files"
            ) as callback:
                self._checkout(
                    path,
                    self.fs,
                    obj,
                    self.cache,
                    relink=True,
                    state=self.repo.state,
                    prompt=prompt.confirm,
                    progress_callback=callback,
                )
            self.set_exec()
        return obj

    @property
    def fspath(self):
        return self.fs_path

    @property
    def is_decorated(self) -> bool:
        return self.is_metric or self.is_plot

    @property
    def is_metric(self) -> bool:
        return bool(self.metric)

    @property
    def is_plot(self) -> bool:
        return bool(self.plot)

    def restore_fields(self, other: "Output"):
        """Restore attributes that need to be preserved when serialized."""
        self.annot = other.annot
        self.remote = other.remote
        self.can_push = other.can_push

    def merge_version_meta(self, other: "Output"):
        """Merge version meta for files which are unchanged from other."""
        if not self.hash_info:
            return
        if self.hash_info.isdir:
            return self._merge_dir_version_meta(other)
        if self.hash_info != other.hash_info:
            return
        self.meta = other.meta

    def _merge_dir_version_meta(self, other: "Output"):
        from dvc_data.hashfile.tree import update_meta

        if not self.obj or not other.hash_info.isdir:
            return
        other_obj = other.obj if other.obj is not None else other.get_obj()
        assert isinstance(self.obj, Tree)
        assert isinstance(other_obj, Tree)
        updated = update_meta(self.obj, other_obj)
        assert updated.hash_info == self.obj.hash_info
        self.obj = updated
        self.files = updated.as_list(with_meta=True)


META_SCHEMA = {
    Meta.PARAM_SIZE: int,
    Meta.PARAM_NFILES: int,
    Meta.PARAM_ISEXEC: bool,
    Meta.PARAM_VERSION_ID: str,
}

CLOUD_SCHEMA = vol.All({str: META_SCHEMA | CHECKSUMS_SCHEMA}, vol.Length(max=1))

ARTIFACT_SCHEMA: dict[Any, Any] = {
    **CHECKSUMS_SCHEMA,
    **META_SCHEMA,
    Output.PARAM_PATH: str,
    Output.PARAM_PERSIST: bool,
    Output.PARAM_CLOUD: CLOUD_SCHEMA,
    Output.PARAM_HASH: str,
}

DIR_FILES_SCHEMA: dict[Any, Any] = {
    **CHECKSUMS_SCHEMA,
    **META_SCHEMA,
    vol.Required(Tree.PARAM_RELPATH): str,
    Output.PARAM_CLOUD: CLOUD_SCHEMA,
}

SCHEMA = {
    **ARTIFACT_SCHEMA,
    **ANNOTATION_SCHEMA,
    Output.PARAM_CACHE: bool,
    Output.PARAM_REMOTE: str,
    Output.PARAM_PUSH: bool,
    Output.PARAM_FILES: [DIR_FILES_SCHEMA],
    Output.PARAM_FS_CONFIG: dict,
}
