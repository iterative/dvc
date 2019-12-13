from __future__ import unicode_literals

import logging
from copy import copy

from voluptuous import Any

import dvc.prompt as prompt
from dvc.cache import NamedCache
from dvc.exceptions import CollectCacheError
from dvc.exceptions import DvcException
from dvc.remote.base import RemoteBASE
from dvc.utils.compat import str
from dvc.utils.compat import urlparse


logger = logging.getLogger(__name__)


class OutputDoesNotExistError(DvcException):
    def __init__(self, path):
        msg = "output '{}' does not exist".format(path)
        super(OutputDoesNotExistError, self).__init__(msg)


class OutputIsNotFileOrDirError(DvcException):
    def __init__(self, path):
        msg = "output '{}' is not a file or directory".format(path)
        super(OutputIsNotFileOrDirError, self).__init__(msg)


class OutputAlreadyTrackedError(DvcException):
    def __init__(self, path):
        msg = "output '{}' is already tracked by scm (e.g. git)".format(path)
        super(OutputAlreadyTrackedError, self).__init__(msg)


class OutputIsStageFileError(DvcException):
    def __init__(self, path):
        super(OutputIsStageFileError, self).__init__(
            "Stage file '{}' cannot be an output.".format(path)
        )


class OutputBase(object):
    IS_DEPENDENCY = False

    REMOTE = RemoteBASE

    PARAM_PATH = "path"
    PARAM_CACHE = "cache"
    PARAM_METRIC = "metric"
    PARAM_METRIC_TYPE = "type"
    PARAM_METRIC_XPATH = "xpath"
    PARAM_PERSIST = "persist"

    METRIC_SCHEMA = Any(
        None,
        bool,
        {
            PARAM_METRIC_TYPE: Any(str, None),
            PARAM_METRIC_XPATH: Any(str, None),
        },
    )

    PARAM_TAGS = "tags"

    DoesNotExistError = OutputDoesNotExistError
    IsNotFileOrDirError = OutputIsNotFileOrDirError
    IsStageFileError = OutputIsStageFileError

    sep = "/"

    def __init__(
        self,
        stage,
        path,
        info=None,
        remote=None,
        cache=True,
        metric=False,
        persist=False,
        tags=None,
    ):
        self._validate_output_path(path)
        # This output (and dependency) objects have too many paths/urls
        # here is a list and comments:
        #
        #   .def_path - path from definition in stage file
        #   .path_info - PathInfo/URLInfo structured resolved path
        #   .fspath - local only, resolved
        #   .__str__ - for presentation purposes, def_path/relpath
        #
        # By resolved path, which contains actual location,
        # should be absolute and don't contain remote:// refs.
        self.stage = stage
        self.repo = stage.repo if stage else None
        self.def_path = path
        self.info = info
        self.remote = remote or self.REMOTE(self.repo, {})
        self.use_cache = False if self.IS_DEPENDENCY else cache
        self.metric = False if self.IS_DEPENDENCY else metric
        self.persist = persist
        self.tags = None if self.IS_DEPENDENCY else (tags or {})

        if self.use_cache and self.cache is None:
            raise DvcException(
                "no cache location setup for '{}' outputs.".format(
                    self.REMOTE.scheme
                )
            )

        self.path_info = self._parse_path(remote, path)

    def _parse_path(self, remote, path):
        if remote:
            parsed = urlparse(path)
            return remote.path_info / parsed.path.lstrip("/")
        return self.REMOTE.path_cls(path)

    def __repr__(self):
        return "{class_name}: '{def_path}'".format(
            class_name=type(self).__name__, def_path=self.def_path
        )

    def __str__(self):
        return self.def_path

    @property
    def scheme(self):
        return self.REMOTE.scheme

    @property
    def is_in_repo(self):
        return False

    @property
    def use_scm_ignore(self):
        if not self.is_in_repo:
            return False

        return self.use_cache or self.stage.is_repo_import

    @property
    def cache(self):
        return getattr(self.repo.cache, self.scheme)

    @property
    def dir_cache(self):
        return self.cache.get_dir_cache(self.checksum)

    @classmethod
    def supported(cls, url):
        return cls.REMOTE.supported(url)

    @property
    def cache_path(self):
        return self.cache.checksum_to_path_info(self.checksum).url

    @property
    def checksum(self):
        return self.info.get(self.remote.PARAM_CHECKSUM)

    @property
    def is_dir_checksum(self):
        return self.remote.is_dir_checksum(self.checksum)

    @property
    def exists(self):
        return self.remote.exists(self.path_info)

    def changed_checksum(self):
        return self.checksum != self.remote.get_checksum(self.path_info)

    def changed_cache(self):
        if not self.use_cache or not self.checksum:
            return True

        return self.cache.changed_cache(self.checksum)

    def status(self):
        if self.checksum and self.use_cache and self.changed_cache():
            return {str(self): "not in cache"}

        if not self.exists:
            return {str(self): "deleted"}

        if self.changed_checksum():
            return {str(self): "modified"}

        if not self.checksum:
            return {str(self): "new"}

        return {}

    def changed(self):
        status = self.status()
        logger.debug(str(status))
        return bool(status)

    @property
    def is_empty(self):
        return self.remote.is_empty(self.path_info)

    def isdir(self):
        return self.remote.isdir(self.path_info)

    def isfile(self):
        return self.remote.isfile(self.path_info)

    def save(self):
        if not self.exists:
            raise self.DoesNotExistError(self)

        if not self.isfile and not self.isdir:
            raise self.IsNotFileOrDirError(self)

        if self.is_empty:
            logger.warning("'{}' is empty.".format(self))

        if self.use_scm_ignore:
            if self.repo.scm.is_tracked(self.fspath):
                raise OutputAlreadyTrackedError(self)

            self.repo.scm.ignore(self.fspath)

        if not self.use_cache:
            self.info = self.remote.save_info(self.path_info)
            if self.metric:
                self.verify_metric()
            if not self.IS_DEPENDENCY:
                logger.info(
                    "Output '{}' doesn't use cache. Skipping saving.".format(
                        self
                    )
                )
            return

        assert not self.IS_DEPENDENCY

        if not self.changed():
            logger.info(
                "Output '{}' didn't change. Skipping saving.".format(self)
            )
            return

        self.info = self.remote.save_info(self.path_info)

    def commit(self):
        if self.use_cache:
            self.cache.save(self.path_info, self.info)

    def dumpd(self):
        ret = copy(self.info)
        ret[self.PARAM_PATH] = self.def_path

        if self.IS_DEPENDENCY:
            return ret

        ret[self.PARAM_CACHE] = self.use_cache

        if isinstance(self.metric, dict):
            if (
                self.PARAM_METRIC_XPATH in self.metric
                and not self.metric[self.PARAM_METRIC_XPATH]
            ):
                del self.metric[self.PARAM_METRIC_XPATH]

        ret[self.PARAM_METRIC] = self.metric
        ret[self.PARAM_PERSIST] = self.persist

        if self.tags:
            ret[self.PARAM_TAGS] = self.tags

        return ret

    def verify_metric(self):
        raise DvcException(
            "verify metric is not supported for {}".format(self.scheme)
        )

    def download(self, to):
        self.remote.download(self.path_info, to.path_info)

    def checkout(
        self, force=False, progress_callback=None, tag=None, relink=False
    ):
        if not self.use_cache:
            if progress_callback:
                progress_callback(str(self.path_info), self.get_files_number())
            return None

        if tag:
            info = self.tags[tag]
        else:
            info = self.info

        return self.cache.checkout(
            self.path_info,
            info,
            force=force,
            progress_callback=progress_callback,
            relink=relink,
        )

    def remove(self, ignore_remove=False):
        self.remote.remove(self.path_info)
        if self.scheme != "local":
            return

        if ignore_remove and self.use_scm_ignore:
            self.repo.scm.ignore_remove(self.fspath)

    def move(self, out):
        if self.scheme == "local" and self.use_scm_ignore:
            self.repo.scm.ignore_remove(self.fspath)

        self.remote.move(self.path_info, out.path_info)
        self.def_path = out.def_path
        self.path_info = out.path_info
        self.save()
        self.commit()

        if self.scheme == "local" and self.use_scm_ignore:
            self.repo.scm.ignore(self.fspath)

    def get_files_number(self):
        if not self.use_cache:
            return 0

        return self.cache.get_files_number(self.checksum)

    def unprotect(self):
        if self.exists:
            self.remote.unprotect(self.path_info)

    def _collect_used_dir_cache(self, remote=None, force=False, jobs=None):
        """Get a list of `info`s related to the given directory.

        - Pull the directory entry from the remote cache if it was changed.

        Example:

            Given the following commands:

            $ echo "foo" > directory/foo
            $ echo "bar" > directory/bar
            $ dvc add directory

            It will return a NamedCache like:

            nc = NamedCache()
            nc.add(self.scheme, 'c157a79031e1', 'directory/foo')
            nc.add(self.scheme, 'd3b07384d113', 'directory/bar')
        """

        cache = NamedCache()

        if self.cache.changed_cache_file(self.checksum):
            try:
                self.repo.cloud.pull(
                    NamedCache.make("local", self.checksum, str(self)),
                    jobs=jobs,
                    remote=remote,
                    show_checksums=False,
                )
            except DvcException:
                logger.debug("failed to pull cache for '{}'".format(self))

        if self.cache.changed_cache_file(self.checksum):
            msg = (
                "Missing cache for directory '{}'. "
                "Cache for files inside will be lost. "
                "Would you like to continue? Use '-f' to force."
            )
            if not force and not prompt.confirm(msg.format(self.path_info)):
                raise CollectCacheError(
                    "unable to fully collect used cache"
                    " without cache for directory '{}'".format(self)
                )
            else:
                return cache

        for entry in self.dir_cache:
            checksum = entry[self.remote.PARAM_CHECKSUM]
            path_info = self.path_info / entry[self.remote.PARAM_RELPATH]
            cache.add(self.scheme, checksum, str(path_info))

        return cache

    def get_used_cache(self, **kwargs):
        """Get a dumpd of the given `out`, with an entry including the branch.

        The `used_cache` of an output is no more than its `info`.

        In case that the given output is a directory, it will also
        include the `info` of its files.
        """

        if not self.use_cache:
            return NamedCache()

        if not self.info:
            logger.warning(
                "Output '{}'({}) is missing version info. Cache for it will "
                "not be collected. Use dvc repro to get your pipeline up to "
                "date.".format(self, self.stage)
            )
            return NamedCache()

        ret = NamedCache.make(self.scheme, self.checksum, str(self))

        if not self.is_dir_checksum:
            return ret

        ret.update(self._collect_used_dir_cache(**kwargs))

        return ret

    @classmethod
    def _validate_output_path(cls, path):
        from dvc.stage import Stage

        if Stage.is_valid_filename(path):
            raise cls.IsStageFileError(path)
