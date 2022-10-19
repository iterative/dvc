import logging
import os
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple, Type
from urllib.parse import urlparse

from funcy import cached_property, collecting, project
from voluptuous import And, Any, Coerce, Length, Lower, Required, SetTo

from dvc import prompt
from dvc.exceptions import (
    CacheLinkError,
    CheckoutError,
    CollectCacheError,
    ConfirmRemoveError,
    DvcException,
    MergeError,
    RemoteCacheRequiredError,
)
from dvc_data.hashfile import Tree
from dvc_data.hashfile import check as ocheck
from dvc_data.hashfile import load as oload
from dvc_data.hashfile.build import build
from dvc_data.hashfile.checkout import checkout
from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.istextfile import istextfile
from dvc_data.hashfile.meta import Meta
from dvc_data.hashfile.transfer import transfer as otransfer
from dvc_data.index import DataIndexEntry
from dvc_objects.errors import ObjectFormatError

from .annotations import ANNOTATION_FIELDS, ANNOTATION_SCHEMA, Annotation
from .fs import LocalFileSystem, RemoteMissingDepsError, Schemes, get_cloud_fs
from .fs.callbacks import DEFAULT_CALLBACK
from .utils import relpath
from .utils.fs import path_isin

if TYPE_CHECKING:
    from dvc_data.hashfile.obj import HashFile
    from dvc_data.index import DataIndexKey
    from dvc_objects.db import ObjectDB

    from .fs.callbacks import Callback

logger = logging.getLogger(__name__)


CHECKSUM_SCHEMA = Any(
    None,
    And(str, Length(max=0), SetTo(None)),
    And(Any(str, And(int, Coerce(str))), Length(min=3), Lower),
)

CASE_SENSITIVE_CHECKSUM_SCHEMA = Any(
    None,
    And(str, Length(max=0), SetTo(None)),
    And(Any(str, And(int, Coerce(str))), Length(min=3)),
)

# NOTE: currently there are only 3 possible checksum names:
#
#    1) md5 (LOCAL, SSH);
#    2) etag (S3, GS, OSS, AZURE, HTTP);
#    3) checksum (HDFS);
#
# so when a few types of outputs share the same name, we only need
# specify it once.
HDFS_PARAM_CHECKSUM = "checksum"
S3_PARAM_CHECKSUM = "etag"
CHECKSUMS_SCHEMA = {
    LocalFileSystem.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
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
        checkpoint = d.pop(Output.PARAM_CHECKPOINT, False)
        live = d.pop(Output.PARAM_LIVE, False)
        remote = d.pop(Output.PARAM_REMOTE, None)
        annot = {field: d.pop(field, None) for field in ANNOTATION_FIELDS}
        files = d.pop(Output.PARAM_FILES, None)
        ret.append(
            _get(
                stage,
                p,
                info=d,
                cache=cache,
                metric=metric,
                plot=plot,
                persist=persist,
                checkpoint=checkpoint,
                live=live,
                remote=remote,
                **annot,
                files=files,
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
    checkpoint=False,
    live=False,
    remote=None,
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
            checkpoint=checkpoint,
            live=live,
            remote=remote,
        )
        for s in s_list
    ]


def _split_dict(d, keys):
    return project(d, keys), project(d, d.keys() - keys)


def _merge_data(s_list):
    d = defaultdict(dict)
    for key in s_list:
        if isinstance(key, str):
            d[key].update({})
            continue
        if not isinstance(key, dict):
            raise ValueError(f"'{type(key).__name__}' not supported.")

        for k, flags in key.items():
            if not isinstance(flags, dict):
                raise ValueError(
                    f"Expected dict for '{k}', got: '{type(flags).__name__}'"
                )
            d[k].update(flags)
    return d


@collecting
def load_from_pipeline(stage, data, typ="outs"):
    if typ not in (
        stage.PARAM_OUTS,
        stage.PARAM_METRICS,
        stage.PARAM_PLOTS,
        stage.PARAM_LIVE,
    ):
        raise ValueError(f"'{typ}' key is not allowed for pipeline files.")

    metric = typ == stage.PARAM_METRICS
    plot = typ == stage.PARAM_PLOTS
    live = typ == stage.PARAM_LIVE
    if live:
        # `live` is single object
        data = [data]

    d = _merge_data(data)

    for path, flags in d.items():
        plt_d, live_d = {}, {}
        if plot:
            from dvc.schema import PLOT_PROPS

            plt_d, flags = _split_dict(flags, keys=PLOT_PROPS.keys())
        if live:
            from dvc.schema import LIVE_PROPS

            live_d, flags = _split_dict(flags, keys=LIVE_PROPS.keys())
        extra = project(
            flags,
            [
                Output.PARAM_CACHE,
                Output.PARAM_PERSIST,
                Output.PARAM_CHECKPOINT,
                Output.PARAM_REMOTE,
                *ANNOTATION_FIELDS,
            ],
        )

        yield _get(
            stage,
            path,
            info={},
            plot=plt_d or plot,
            metric=metric,
            live=live_d or live,
            **extra,
        )


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


class Output:
    IS_DEPENDENCY = False

    PARAM_PATH = "path"
    PARAM_CACHE = "cache"
    PARAM_CHECKPOINT = "checkpoint"
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
    PARAM_LIVE = "live"
    PARAM_LIVE_SUMMARY = "summary"
    PARAM_LIVE_HTML = "html"
    PARAM_REMOTE = "remote"

    METRIC_SCHEMA = Any(
        None,
        bool,
        {
            PARAM_METRIC_TYPE: Any(str, None),
            PARAM_METRIC_XPATH: Any(str, None),
        },
    )

    DoesNotExistError = OutputDoesNotExistError  # type: Type[DvcException]
    IsNotFileOrDirError = OutputIsNotFileOrDirError  # type: Type[DvcException]
    IsStageFileError = OutputIsStageFileError  # type: Type[DvcException]
    IsIgnoredError = OutputIsIgnoredError  # type: Type[DvcException]

    def __init__(
        self,
        stage,
        path,
        info=None,
        cache=True,
        metric=False,
        plot=False,
        persist=False,
        checkpoint=False,
        live=False,
        desc=None,
        type=None,  # pylint: disable=redefined-builtin
        labels=None,
        meta=None,
        remote=None,
        repo=None,
        fs_config=None,
        files: List[Dict[str, Any]] = None,
    ):
        self.annot = Annotation(
            desc=desc, type=type, labels=labels or [], meta=meta or {}
        )
        self.repo = stage.repo if not repo and stage else repo
        meta = Meta.from_dict(info or {})
        # NOTE: when version_aware is not passed into get_cloud_fs, it will be
        # set based on whether or not path is versioned
        fs_kwargs = {"version_aware": True} if meta.version_id else {}

        if fs_config is not None:
            fs_kwargs.update(**fs_config)

        fs_cls, fs_config, fs_path = get_cloud_fs(
            self.repo, url=path, **fs_kwargs
        )
        self.fs = fs_cls(**fs_config)

        if (
            self.fs.protocol == "local"
            and stage
            and isinstance(stage.repo.fs, LocalFileSystem)
            and path_isin(path, stage.repo.root_dir)
        ):
            self.def_path = relpath(path, stage.wdir)
            self.fs = stage.repo.fs
        else:
            self.def_path = path

        if (
            self.repo
            and self.fs.protocol == "local"
            and not self.fs.path.isabs(self.def_path)
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
        self.files = files
        self.use_cache = False if self.IS_DEPENDENCY else cache
        self.metric = False if self.IS_DEPENDENCY else metric
        self.plot = False if self.IS_DEPENDENCY else plot
        self.persist = persist
        self.checkpoint = checkpoint
        self.live = live

        self.fs_path = self._parse_path(self.fs, fs_path)
        self.obj: Optional["HashFile"] = None

        self.remote = remote

        if self.fs.version_aware:
            _, version_id = self.fs.path.coalesce_version(
                self.def_path, self.meta.version_id
            )
            self.meta.version_id = version_id

        if self.is_in_repo:
            self.hash_name = "md5"
        else:
            self.hash_name = self.fs.PARAM_CHECKSUM

        self.hash_info = HashInfo(
            name=self.hash_name,
            value=getattr(self.meta, self.hash_name, None),
        )
        if self.hash_info and self.hash_info.isdir:
            self.meta.isdir = True

    def _parse_path(self, fs, fs_path):
        parsed = urlparse(self.def_path)
        if (
            parsed.scheme != "remote"
            and self.stage
            and self.stage.repo.fs == fs
            and not fs.path.isabs(fs_path)
        ):
            # NOTE: we can path either from command line or .dvc file,
            # so we should expect both posix and windows style paths.
            # paths accepts both, i.e. / works everywhere, \ only on win.
            #
            # FIXME: if we have Windows path containing / or posix one with \
            # then we have #2059 bug and can't really handle that.
            fs_path = fs.path.join(self.stage.wdir, fs_path)

        return fs.path.abspath(fs.path.normpath(fs_path))

    def __repr__(self):
        return "{class_name}: '{def_path}'".format(
            class_name=type(self).__name__, def_path=self.def_path
        )

    def __str__(self):
        if self.fs.protocol != "local":
            return self.def_path

        if (
            not self.repo
            or urlparse(self.def_path).scheme == "remote"
            or os.path.isabs(self.def_path)
        ):
            return str(self.def_path)

        cur_dir = self.fs.path.getcwd()
        if self.fs.path.isin(cur_dir, self.repo.root_dir):
            return self.fs.path.relpath(self.fs_path, cur_dir)

        return self.fs.path.relpath(self.fs_path, self.repo.root_dir)

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

        if self.fs.path.isabs(self.def_path):
            return False

        return self.repo and self.fs.path.isin(
            self.fs.path.realpath(self.fs_path),
            self.repo.root_dir,
        )

    @property
    def use_scm_ignore(self):
        if not self.is_in_repo:
            return False

        return self.use_cache or self.stage.is_repo_import

    @property
    def odb(self):
        odb_name = "repo" if self.is_in_repo else self.protocol
        odb = getattr(self.repo.odb, odb_name)
        if self.use_cache and odb is None:
            raise RemoteCacheRequiredError(self.fs.protocol, self.fs_path)
        return odb

    @property
    def cache_path(self):
        return self.odb.fs.unstrip_protocol(
            self.odb.oid_to_path(self.hash_info.value)
        )

    def get_hash(self):
        _, hash_info = self._get_hash_meta()
        return hash_info

    def _get_hash_meta(self):
        if self.use_cache:
            odb = self.odb
        else:
            odb = self.repo.odb.local
        _, meta, obj = build(
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
        if not self.IS_DEPENDENCY and self.dvcignore:
            if self.dvcignore.is_ignored(self.fs, path, ignore_subrepos=False):
                return True
        return False

    @property
    def exists(self):
        if self._is_path_dvcignore(self.fs_path):
            return False

        return self.fs.exists(self.fs_path)

    @cached_property
    def index_key(self) -> Tuple[str, "DataIndexKey"]:
        if self.is_in_repo:
            workspace = "repo"
            key = self.repo.fs.path.relparts(self.fs_path, self.repo.root_dir)
        else:
            workspace = self.fs.protocol
            no_drive = self.fs.path.flavour.splitdrive(self.fs_path)[1]
            key = self.fs.path.parts(no_drive)[1:]
        return workspace, key

    def get_entry(self) -> "DataIndexEntry":
        from dvc.config import NoRemoteError

        try:
            remote = self.repo.cloud.get_remote_odb(self.remote)
        except NoRemoteError:
            remote = None

        return DataIndexEntry(
            meta=self.meta,
            obj=self.obj,
            hash_info=self.hash_info,
            odb=self.odb,
            cache=self.odb,
            remote=remote,
        )

    def changed_checksum(self):
        return self.hash_info != self.get_hash()

    def changed_cache(self, filter_info=None):
        if not self.use_cache or not self.hash_info:
            return True

        obj = self.get_obj(filter_info=filter_info)
        if not obj:
            return True

        try:
            ocheck(self.odb, obj)
            return False
        except (FileNotFoundError, ObjectFormatError):
            return True

    def changed_meta(self) -> bool:
        if self.fs.version_aware and self.meta.version_id:
            return self.meta.version_id != self.get_meta().version_id
        return False

    def workspace_status(self):
        if not self.exists:
            return {str(self): "deleted"}

        if self.changed_checksum():
            return {str(self): "modified"}

        if not self.hash_info:
            return {str(self): "new"}

        return {}

    def status(self):
        if self.hash_info and self.use_cache and self.changed_cache():
            return {str(self): "not in cache"}

        return self.workspace_status()

    def changed(self):
        status = self.status()
        logger.debug(str(status))
        return bool(status)

    @property
    def dvcignore(self):
        if self.fs.protocol == "local":
            return self.repo.dvcignore
        return None

    @property
    def is_empty(self):
        return self.fs.is_empty(self.fs_path)

    def isdir(self):
        if self._is_path_dvcignore(self.fs_path):
            return False
        return self.fs.isdir(self.fs_path)

    def isfile(self):
        if self._is_path_dvcignore(self.fs_path):
            return False
        return self.fs.isfile(self.fs_path)

    # pylint: disable=no-member

    def ignore(self):
        if not self.use_scm_ignore:
            return

        if self.repo.scm.is_tracked(self.fspath):
            raise OutputAlreadyTrackedError(self)

        self.repo.scm_context.ignore(self.fspath)

    def ignore_remove(self):
        if not self.use_scm_ignore:
            return

        self.repo.scm_context.ignore_remove(self.fspath)

    # pylint: enable=no-member

    def save(self):
        if not self.exists:
            raise self.DoesNotExistError(self)

        if not self.isfile and not self.isdir:
            raise self.IsNotFileOrDirError(self)

        if self.is_empty:
            logger.warning("'%s' is empty.", self)

        self.ignore()

        if self.metric:
            self.verify_metric()

        if not self.use_cache:
            _, self.meta, obj = build(
                self.repo.odb.local,
                self.fs_path,
                self.fs,
                self.hash_name,
                ignore=self.dvcignore,
                dry_run=True,
            )
            self.hash_info = obj.hash_info
            self.files = None
            if not self.IS_DEPENDENCY:
                logger.debug(
                    "Output '%s' doesn't use cache. Skipping saving.", self
                )
            return

        assert not self.IS_DEPENDENCY

        _, self.meta, self.obj = build(
            self.odb,
            self.fs_path,
            self.fs,
            self.hash_name,
            ignore=self.dvcignore,
        )
        self.hash_info = self.obj.hash_info
        self.files = None

    def set_exec(self):
        if self.isfile() and self.meta.isexec:
            self.odb.set_exec(self.fs_path)

    def _checkout(self, *args, **kwargs):
        from dvc_data.hashfile.checkout import CheckoutError as _CheckoutError
        from dvc_data.hashfile.checkout import LinkError, PromptError

        kwargs.setdefault("ignore", self.dvcignore)
        try:
            return checkout(*args, **kwargs)
        except PromptError as exc:
            raise ConfirmRemoveError(exc.path)
        except LinkError as exc:
            raise CacheLinkError([exc.path])
        except _CheckoutError as exc:
            raise CheckoutError(exc.paths)

    def commit(self, filter_info=None):
        if not self.exists:
            raise self.DoesNotExistError(self)

        assert self.hash_info

        if self.use_cache:
            granular = (
                self.is_dir_checksum
                and filter_info
                and filter_info != self.fs_path
            )
            if granular:
                obj = self._commit_granular_dir(filter_info)
            else:
                staging, _, obj = build(
                    self.odb,
                    filter_info or self.fs_path,
                    self.fs,
                    self.hash_name,
                    ignore=self.dvcignore,
                )
                otransfer(
                    staging,
                    self.odb,
                    {obj.hash_info},
                    shallow=False,
                    hardlink=True,
                )
            self._checkout(
                filter_info or self.fs_path,
                self.fs,
                obj,
                self.odb,
                relink=True,
                state=self.repo.state,
                prompt=prompt.confirm,
            )
            self.set_exec()

    def _commit_granular_dir(self, filter_info):
        prefix = self.fs.path.parts(
            self.fs.path.relpath(filter_info, self.fs_path)
        )
        staging, _, save_obj = build(
            self.odb,
            self.fs_path,
            self.fs,
            self.hash_name,
            ignore=self.dvcignore,
        )
        save_obj = save_obj.filter(prefix)
        checkout_obj = save_obj.get_obj(self.odb, prefix)
        otransfer(
            staging,
            self.odb,
            {save_obj.hash_info} | {oid for _, _, oid in save_obj},
            shallow=True,
            hardlink=True,
        )
        return checkout_obj

    def dumpd(self, **kwargs):
        meta = self.meta.to_dict()
        meta.pop("isdir", None)
        ret = {**self.hash_info.to_dict(), **meta}

        if self.is_in_repo:
            path = self.fs.path.as_posix(
                relpath(self.fs_path, self.stage.wdir)
            )
        else:
            path = self.def_path

        ret[self.PARAM_PATH] = path

        if self.IS_DEPENDENCY:
            return ret

        ret.update(self.annot.to_dict())
        if not self.use_cache:
            ret[self.PARAM_CACHE] = self.use_cache

        if isinstance(self.metric, dict):
            if (
                self.PARAM_METRIC_XPATH in self.metric
                and not self.metric[self.PARAM_METRIC_XPATH]
            ):
                del self.metric[self.PARAM_METRIC_XPATH]

        if self.metric:
            ret[self.PARAM_METRIC] = self.metric

        if self.plot:
            ret[self.PARAM_PLOT] = self.plot

        if self.persist:
            ret[self.PARAM_PERSIST] = self.persist

        if self.checkpoint:
            ret[self.PARAM_CHECKPOINT] = self.checkpoint

        if self.live:
            ret[self.PARAM_LIVE] = self.live

        if self.remote:
            ret[self.PARAM_REMOTE] = self.remote

        if self.hash_info.isdir and (
            kwargs.get("with_files") or self.files is not None
        ):
            if self.obj:
                obj = self.obj
            else:
                obj = self.get_obj()
            ret[self.PARAM_FILES] = obj.as_list(with_meta=True)

        return ret

    def verify_metric(self):
        if self.fs.protocol != "local":
            raise DvcException(
                f"verify metric is not supported for {self.protocol}"
            )
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
        obj: Optional["HashFile"] = None
        if self.obj:
            obj = self.obj
        elif self.hash_info:
            if self.files:
                tree = Tree.from_list(self.files, hash_name=self.hash_name)
                tree.digest()
                obj = tree
            else:
                try:
                    obj = oload(self.odb, self.hash_info)
                except (FileNotFoundError, ObjectFormatError):
                    return None
        else:
            return None

        assert obj
        fs_path = self.fs.path
        if filter_info and filter_info != self.fs_path:
            prefix = fs_path.relparts(filter_info, self.fs_path)
            obj = obj.get_obj(self.odb, prefix)

        return obj

    def checkout(
        self,
        force=False,
        progress_callback: "Callback" = DEFAULT_CALLBACK,
        relink=False,
        filter_info=None,
        allow_missing=False,
        checkpoint_reset=False,
        **kwargs,
    ):
        if not self.use_cache:
            if progress_callback != DEFAULT_CALLBACK:
                progress_callback.relative_update(
                    self.get_files_number(filter_info)
                )
            return None

        obj = self.get_obj(filter_info=filter_info)
        if not obj and (filter_info and filter_info != self.fs_path):
            # backward compatibility
            return None

        if self.checkpoint and checkpoint_reset:
            if self.exists:
                self.remove()
            return None

        added = not self.exists

        try:
            modified = self._checkout(
                filter_info or self.fs_path,
                self.fs,
                obj,
                self.odb,
                force=force,
                progress_callback=progress_callback,
                relink=relink,
                state=self.repo.state,
                prompt=prompt.confirm,
                **kwargs,
            )
        except CheckoutError:
            if allow_missing or self.checkpoint:
                return None
            raise
        self.set_exec()
        return added, False if added else modified

    def remove(self, ignore_remove=False):
        self.fs.remove(self.fs_path)
        if self.protocol != Schemes.LOCAL:
            return

        if ignore_remove:
            self.ignore_remove()

    def move(self, out):
        # pylint: disable=no-member
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
            odb = self.odb

        cls, config, from_info = get_cloud_fs(self.repo, url=source)
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
        staging, self.meta, obj = build(
            odb,
            from_info,
            from_fs,
            "md5",
            upload=upload,
            no_progress_bar=no_progress_bar,
        )
        otransfer(
            staging,
            odb,
            {obj.hash_info},
            jobs=jobs,
            hardlink=False,
            shallow=False,
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
        if self.exists:
            self.odb.unprotect(self.fs_path)

    def get_dir_cache(self, **kwargs):
        if not self.is_dir_checksum:
            raise DvcException("cannot get dir cache for file checksum")

        obj = self.odb.get(self.hash_info.value)
        try:
            ocheck(self.odb, obj)
        except FileNotFoundError:
            if self.remote:
                kwargs["remote"] = self.remote
            self.repo.cloud.pull([obj.hash_info], **kwargs)

        if self.obj:
            return self.obj

        try:
            self.obj = oload(self.odb, self.hash_info)
        except (FileNotFoundError, ObjectFormatError):
            self.obj = None

        return self.obj

    def _collect_used_dir_cache(
        self, remote=None, force=False, jobs=None, filter_info=None
    ) -> Optional["Tree"]:
        """Fetch dir cache and return used object IDs for this out."""

        try:
            self.get_dir_cache(jobs=jobs, remote=remote)
        except RemoteMissingDepsError:  # pylint: disable=try-except-raise
            raise
        except DvcException:
            logger.debug("failed to pull cache for '%s'", self)

        try:
            ocheck(self.odb, self.odb.get(self.hash_info.value))
        except FileNotFoundError:
            msg = (
                "Missing cache for directory '{}'. "
                "Cache for files inside will be lost. "
                "Would you like to continue? Use '-f' to force."
            )
            if not force and not prompt.confirm(msg.format(self.fs_path)):
                raise CollectCacheError(
                    "unable to fully collect used cache"
                    " without cache for directory '{}'".format(self)
                )
            return None

        obj = self.get_obj()
        if filter_info and filter_info != self.fs_path:
            assert obj
            prefix = self.fs.path.parts(
                self.fs.path.relpath(filter_info, self.fs_path)
            )
            obj = obj.filter(prefix)
        return obj

    def get_used_objs(
        self, **kwargs
    ) -> Dict[Optional["ObjectDB"], Set["HashInfo"]]:
        """Return filtered set of used object IDs for this out."""

        if not self.use_cache:
            return {}

        if self.stage.is_repo_import:
            return self.get_used_external(**kwargs)

        if not self.hash_info:
            msg = (
                "Output '{}'({}) is missing version info. "
                "Cache for it will not be collected. "
                "Use `dvc repro` to get your pipeline up to date.".format(
                    self, self.stage
                )
            )
            if self.exists:
                msg += (
                    "\n"
                    "You can also use `dvc commit {stage.addressing}` "
                    "to associate existing '{out}' with {stage}.".format(
                        out=self, stage=self.stage
                    )
                )
            logger.warning(msg)
            return {}

        if self.is_dir_checksum:
            obj = self._collect_used_dir_cache(**kwargs)
        else:
            obj = self.get_obj(filter_info=kwargs.get("filter_info"))
            if not obj:
                obj = self.odb.get(self.hash_info.value)

        if not obj:
            return {}

        if self.remote:
            remote = self.repo.cloud.get_remote_odb(name=self.remote)
        else:
            remote = None

        return {remote: self._named_obj_ids(obj)}

    def _named_obj_ids(self, obj):
        name = str(self)
        obj.hash_info.obj_name = name
        oids = {obj.hash_info}
        if isinstance(obj, Tree):
            for key, _, oid in obj:
                oid.obj_name = self.fs.sep.join([name, *key])
                oids.add(oid)
        return oids

    def get_used_external(
        self, **kwargs
    ) -> Dict[Optional["ObjectDB"], Set["HashInfo"]]:
        if not self.use_cache or not self.stage.is_repo_import:
            return {}

        (dep,) = self.stage.deps
        return dep.get_used_objs(**kwargs)

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
        ]

        for opt in ignored:
            my.pop(opt, None)
            other.pop(opt, None)

        if my != other:
            raise MergeError(
                "unable to auto-merge outputs with different options"
            )

        if not out.is_dir_checksum:
            raise MergeError(
                "unable to auto-merge outputs that are not directories"
            )

    def merge(self, ancestor, other, allowed=None):
        from dvc_data.hashfile.tree import MergeError as TreeMergeError
        from dvc_data.hashfile.tree import du, merge

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
                self.odb,
                ancestor_info,
                self.hash_info,
                other.hash_info,
                allowed=allowed,
            )
        except TreeMergeError as exc:
            raise MergeError(str(exc)) from exc

        self.odb.add(merged.path, merged.fs, merged.oid)

        self.hash_info = merged.hash_info
        self.files = None
        self.meta = Meta(
            size=du(self.odb, merged),
            nfiles=len(merged),
        )

    @property
    def fspath(self):
        return self.fs_path

    @property
    def is_decorated(self) -> bool:
        return self.is_metric or self.is_plot

    @property
    def is_metric(self) -> bool:
        return bool(self.metric) or bool(self.live)

    @property
    def is_plot(self) -> bool:
        return bool(self.plot) or bool(self.live)


META_SCHEMA = {
    Meta.PARAM_SIZE: int,
    Meta.PARAM_NFILES: int,
    Meta.PARAM_ISEXEC: bool,
    Meta.PARAM_VERSION_ID: str,
}

ARTIFACT_SCHEMA = {
    **CHECKSUMS_SCHEMA,
    **META_SCHEMA,
    Required(Output.PARAM_PATH): str,
    Output.PARAM_PLOT: bool,
    Output.PARAM_PERSIST: bool,
    Output.PARAM_CHECKPOINT: bool,
}

DIR_FILES_SCHEMA: Dict[str, Any] = {
    **CHECKSUMS_SCHEMA,
    **META_SCHEMA,
    Required(Tree.PARAM_RELPATH): str,
}

SCHEMA = {
    **ARTIFACT_SCHEMA,
    **ANNOTATION_SCHEMA,
    Output.PARAM_CACHE: bool,
    Output.PARAM_METRIC: Output.METRIC_SCHEMA,
    Output.PARAM_REMOTE: str,
    Output.PARAM_FILES: [DIR_FILES_SCHEMA],
}
