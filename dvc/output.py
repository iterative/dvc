import logging
import os
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, Optional, Set, Type
from urllib.parse import urlparse

from funcy import collecting, project
from voluptuous import And, Any, Coerce, Length, Lower, Required, SetTo

from dvc import prompt
from dvc.exceptions import (
    CheckoutError,
    CollectCacheError,
    DvcException,
    MergeError,
    RemoteCacheRequiredError,
)

from .data import check as ocheck
from .data import load as oload
from .data.checkout import checkout
from .data.meta import Meta
from .data.stage import stage as ostage
from .data.transfer import transfer as otransfer
from .data.tree import Tree
from .fs import get_cloud_fs
from .fs.hdfs import HDFSFileSystem
from .fs.local import LocalFileSystem
from .fs.s3 import S3FileSystem
from .hash_info import HashInfo
from .istextfile import istextfile
from .objects.errors import ObjectFormatError
from .scheme import Schemes
from .utils import relpath
from .utils.fs import path_isin

if TYPE_CHECKING:
    from .objects.db import ObjectDB

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
CHECKSUMS_SCHEMA = {
    LocalFileSystem.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    HDFSFileSystem.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    S3FileSystem.PARAM_CHECKSUM: CASE_SENSITIVE_CHECKSUM_SCHEMA,
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
        desc = d.pop(Output.PARAM_DESC, False)
        live = d.pop(Output.PARAM_LIVE, False)
        remote = d.pop(Output.PARAM_REMOTE, None)
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
                desc=desc,
                live=live,
                remote=remote,
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
    PARAM_DESC = "desc"
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
        remote=None,
        repo=None,
    ):
        self.repo = stage.repo if not repo and stage else repo
        fs_cls, fs_config, fs_path = get_cloud_fs(self.repo, url=path)
        self.fs = fs_cls(**fs_config)

        if (
            self.fs.scheme == "local"
            and stage
            and path_isin(path, stage.repo.root_dir)
        ):
            self.def_path = relpath(path, stage.wdir)
        else:
            self.def_path = path

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
        self.meta = Meta.from_dict(info)
        self.hash_info = HashInfo.from_dict(info)
        self.use_cache = False if self.IS_DEPENDENCY else cache
        self.metric = False if self.IS_DEPENDENCY else metric
        self.plot = False if self.IS_DEPENDENCY else plot
        self.persist = persist
        self.checkpoint = checkpoint
        self.live = live
        self.desc = desc

        self.fs_path = self._parse_path(self.fs, fs_path)
        self.obj = None

        self.remote = remote

    def _parse_path(self, fs, fs_path):
        if fs.scheme != "local":
            return fs_path

        parsed = urlparse(self.def_path)
        if parsed.scheme != "remote":
            # NOTE: we can path either from command line or .dvc file,
            # so we should expect both posix and windows style paths.
            # paths accepts both, i.e. / works everywhere, \ only on win.
            #
            # FIXME: if we have Windows path containing / or posix one with \
            # then we have #2059 bug and can't really handle that.
            if self.stage and not os.path.isabs(fs_path):
                fs_path = fs.path.join(self.stage.wdir, fs_path)

        abs_p = os.path.abspath(os.path.normpath(fs_path))
        return abs_p

    def __repr__(self):
        return "{class_name}: '{def_path}'".format(
            class_name=type(self).__name__, def_path=self.def_path
        )

    def __str__(self):
        if self.fs.scheme != "local":
            return self.def_path

        if (
            not self.repo
            or urlparse(self.def_path).scheme == "remote"
            or os.path.isabs(self.def_path)
        ):
            return str(self.def_path)

        cur_dir = os.getcwd()
        if path_isin(cur_dir, self.repo.root_dir):
            return relpath(self.fs_path, cur_dir)

        return relpath(self.fs_path, self.repo.root_dir)

    @property
    def scheme(self):
        return self.fs.scheme

    @property
    def is_in_repo(self):
        if self.fs.scheme != "local":
            return False

        if urlparse(self.def_path).scheme == "remote":
            return False

        if os.path.isabs(self.def_path):
            return False

        return self.repo and path_isin(
            os.path.realpath(self.fs_path), self.repo.root_dir
        )

    @property
    def use_scm_ignore(self):
        if not self.is_in_repo:
            return False

        return self.use_cache or self.stage.is_repo_import

    @property
    def odb(self):
        odb = getattr(self.repo.odb, self.scheme)
        if self.use_cache and odb is None:
            raise RemoteCacheRequiredError(self.fs.scheme, self.fs_path)
        return odb

    @property
    def cache_path(self):
        return self.odb.fs.unstrip_protocol(
            self.odb.hash_to_path(self.hash_info.value)
        )

    def get_hash(self):
        if self.use_cache:
            odb = self.odb
            name = self.odb.fs.PARAM_CHECKSUM
        else:
            odb = self.repo.odb.local
            name = self.fs.PARAM_CHECKSUM
        _, _, obj = ostage(
            odb,
            self.fs_path,
            self.fs,
            name,
            dvcignore=self.dvcignore,
            dry_run=not self.use_cache,
        )
        return obj.hash_info

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
        if self.fs.scheme == "local":
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
            logger.warning(f"'{self}' is empty.")

        self.ignore()

        if self.metric or self.plot:
            self.verify_metric()

        if not self.use_cache:
            _, self.meta, obj = ostage(
                self.repo.odb.local,
                self.fs_path,
                self.fs,
                self.fs.PARAM_CHECKSUM,
                dvcignore=self.dvcignore,
                dry_run=True,
            )
            self.hash_info = obj.hash_info
            if not self.IS_DEPENDENCY:
                logger.debug(
                    "Output '%s' doesn't use cache. Skipping saving.", self
                )
            return

        assert not self.IS_DEPENDENCY

        _, self.meta, self.obj = ostage(
            self.odb,
            self.fs_path,
            self.fs,
            self.odb.fs.PARAM_CHECKSUM,
            dvcignore=self.dvcignore,
        )
        self.hash_info = self.obj.hash_info

    def set_exec(self):
        if self.isfile() and self.meta.isexec:
            self.odb.set_exec(self.fs_path)

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
                staging, _, obj = ostage(
                    self.odb,
                    filter_info or self.fs_path,
                    self.fs,
                    self.odb.fs.PARAM_CHECKSUM,
                    dvcignore=self.dvcignore,
                )
                otransfer(
                    staging,
                    self.odb,
                    {obj.hash_info},
                    shallow=False,
                    hardlink=True,
                )
            checkout(
                filter_info or self.fs_path,
                self.fs,
                obj,
                self.odb,
                relink=True,
                dvcignore=self.dvcignore,
                state=self.repo.state,
            )
            self.set_exec()

    def _commit_granular_dir(self, filter_info):
        prefix = self.fs.path.parts(
            self.fs.path.relpath(filter_info, self.fs_path)
        )
        staging, _, save_obj = ostage(
            self.odb,
            self.fs_path,
            self.fs,
            self.odb.fs.PARAM_CHECKSUM,
            dvcignore=self.dvcignore,
        )
        save_obj = save_obj.filter(prefix)
        checkout_obj = save_obj.get(self.odb, prefix)
        otransfer(
            staging,
            self.odb,
            {save_obj.hash_info} | {oid for _, _, oid in save_obj},
            shallow=True,
            hardlink=True,
        )
        return checkout_obj

    def dumpd(self):
        ret = {**self.hash_info.to_dict(), **self.meta.to_dict()}

        if self.is_in_repo:
            path = self.fs.path.as_posix(
                relpath(self.fs_path, self.stage.wdir)
            )
        else:
            path = self.def_path

        ret[self.PARAM_PATH] = path

        if self.IS_DEPENDENCY:
            return ret

        if self.desc:
            ret[self.PARAM_DESC] = self.desc

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

        return ret

    def verify_metric(self):
        if self.fs.scheme != "local":
            raise DvcException(
                f"verify metric is not supported for {self.scheme}"
            )

        if not self.metric or self.plot:
            return

        if not os.path.exists(self.fs_path):
            return

        name = "metrics" if self.metric else "plot"
        if os.path.isdir(self.fs_path):
            msg = "directory '%s' cannot be used as %s."
            logger.debug(msg, str(self), name)
            return

        if not istextfile(self.fs_path, self.fs):
            msg = "binary file '{}' cannot be used as {}."
            raise DvcException(msg.format(self.fs_path, name))

    def download(self, to, jobs=None):
        self.fs.download(self.fs_path, to.fs_path, jobs=jobs)

    def get_obj(self, filter_info=None, **kwargs):
        if self.obj:
            obj = self.obj
        elif self.hash_info:
            try:
                obj = oload(self.odb, self.hash_info)
            except FileNotFoundError:
                return None
        else:
            return None

        fs_path = self.fs.path
        if filter_info and filter_info != self.fs_path:
            prefix = fs_path.relparts(filter_info, self.fs_path)
            obj = obj.get(self.odb, prefix)

        return obj

    def checkout(
        self,
        force=False,
        progress_callback=None,
        relink=False,
        filter_info=None,
        allow_missing=False,
        checkpoint_reset=False,
        **kwargs,
    ):
        if not self.use_cache:
            if progress_callback:
                progress_callback(
                    self.fs_path, self.get_files_number(filter_info)
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
            modified = checkout(
                filter_info or self.fs_path,
                self.fs,
                obj,
                self.odb,
                force=force,
                progress_callback=progress_callback,
                relink=relink,
                state=self.repo.state,
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
        if self.scheme != Schemes.LOCAL:
            return

        if ignore_remove:
            self.ignore_remove()

    def move(self, out):
        # pylint: disable=no-member
        if self.scheme == "local" and self.use_scm_ignore:
            self.repo.scm_context.ignore_remove(self.fspath)

        self.fs.move(self.fs_path, out.fs_path)
        self.def_path = out.def_path
        self.fs_path = out.fs_path
        self.save()
        self.commit()

        if self.scheme == "local" and self.use_scm_ignore:
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
        staging, self.meta, obj = ostage(
            odb,
            from_info,
            from_fs,
            "md5",
            upload=upload,
            jobs=jobs,
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

        obj = self.odb.get(self.hash_info)
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
        except DvcException:
            logger.debug(f"failed to pull cache for '{self}'")

        try:
            ocheck(self.odb, self.odb.get(self.hash_info))
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
                obj = self.odb.get(self.hash_info)

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
        if self.scheme != out.scheme:
            raise MergeError("unable to auto-merge outputs of different types")

        my = self.dumpd()
        other = out.dumpd()

        ignored = [
            self.fs.PARAM_CHECKSUM,
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

    def merge(self, ancestor, other):
        from dvc.data.tree import du, merge

        assert other

        if ancestor:
            self._check_can_merge(ancestor)
            ancestor_info = ancestor.hash_info
        else:
            ancestor_info = None

        self._check_can_merge(self)
        self._check_can_merge(other)

        merged = merge(
            self.odb, ancestor_info, self.hash_info, other.hash_info
        )
        self.odb.add(merged.fs_path, merged.fs, merged.hash_info)

        self.hash_info = merged.hash_info
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
        return bool(self.plot)


ARTIFACT_SCHEMA = {
    **CHECKSUMS_SCHEMA,
    Required(Output.PARAM_PATH): str,
    Output.PARAM_PLOT: bool,
    Output.PARAM_PERSIST: bool,
    Output.PARAM_CHECKPOINT: bool,
    Meta.PARAM_SIZE: int,
    Meta.PARAM_NFILES: int,
    Meta.PARAM_ISEXEC: bool,
}

SCHEMA = {
    **ARTIFACT_SCHEMA,
    Output.PARAM_CACHE: bool,
    Output.PARAM_METRIC: Output.METRIC_SCHEMA,
    Output.PARAM_DESC: str,
    Output.PARAM_REMOTE: str,
}
