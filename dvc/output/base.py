from __future__ import unicode_literals

import re
import logging
from copy import copy

from schema import Or, Optional

from dvc.exceptions import DvcException
from dvc.utils.compat import str
from dvc.remote.base import RemoteBase


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


class OutputBase(object):
    IS_DEPENDENCY = False

    REMOTE = RemoteBase

    PARAM_PATH = "path"
    PARAM_CACHE = "cache"
    PARAM_METRIC = "metric"
    PARAM_METRIC_TYPE = "type"
    PARAM_METRIC_XPATH = "xpath"
    PARAM_PERSIST = "persist"

    METRIC_SCHEMA = Or(
        None,
        bool,
        {
            Optional(PARAM_METRIC_TYPE): Or(str, None),
            Optional(PARAM_METRIC_XPATH): Or(str, None),
        },
    )

    PARAM_TAGS = "tags"

    DoesNotExistError = OutputDoesNotExistError
    IsNotFileOrDirError = OutputIsNotFileOrDirError

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
        self.stage = stage
        self.repo = stage.repo
        self.url = path
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

    def __repr__(self):
        return "{class_name}: '{url}'".format(
            class_name=type(self).__name__, url=(self.url or "No url")
        )

    def __str__(self):
        return self.url

    @property
    def is_in_repo(self):
        return False

    @property
    def cache(self):
        return getattr(self.repo.cache, self.scheme)

    @property
    def dir_cache(self):
        return self.cache.get_dir_cache(self.checksum)

    def assign_to_stage_file(self, target_repo):
        raise DvcException(
            "change repo is not supported for {}".format(self.scheme)
        )

    @classmethod
    def match(cls, url):
        return re.match(cls.REMOTE.REGEX, url)

    def group(self, name):
        match = self.match(self.url)
        if not match:
            return None
        return match.group(name)

    @classmethod
    def supported(cls, url):
        return cls.match(url) is not None

    @property
    def scheme(self):
        return self.REMOTE.scheme

    @property
    def path(self):
        return self.path_info.path

    @property
    def cache_path(self):
        return self.cache.checksum_to_path(self.checksum)

    @property
    def sep(self):
        return "/"

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
        return (
            self.checksum
            != self.remote.save_info(self.path_info)[
                self.remote.PARAM_CHECKSUM
            ]
        )

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

        if self.is_in_repo:
            if self.repo.scm.is_tracked(self.path):
                raise OutputAlreadyTrackedError(self)

            if self.use_cache:
                self.repo.scm.ignore(self.path)

        self.info = self.remote.save_info(self.path_info)

    def commit(self):
        if self.use_cache:
            self.cache.save(self.path_info, self.info)

    def dumpd(self):
        ret = copy(self.info)
        ret[self.PARAM_PATH] = self.url

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

    def download(self, to_info, resume=False):
        self.remote.download([self.path_info], [to_info], resume=resume)

    def checkout(self, force=False, progress_callback=None, tag=None):
        if not self.use_cache:
            return

        if tag:
            info = self.tags[tag]
        else:
            info = self.info

        self.cache.checkout(
            self.path_info,
            info,
            force=force,
            progress_callback=progress_callback,
        )

    def remove(self, ignore_remove=False):
        self.remote.remove(self.path_info)
        if self.scheme != "local":
            return

        if ignore_remove and self.use_cache and self.is_in_repo:
            self.repo.scm.ignore_remove(self.path)

    def move(self, out):
        if self.scheme == "local" and self.use_cache and self.is_in_repo:
            self.repo.scm.ignore_remove(self.path)

        self.remote.move(self.path_info, out.path_info)
        self.url = out.url
        self.path_info = copy(out.path_info)
        self.save()
        self.commit()

        if self.scheme == "local" and self.use_cache and self.is_in_repo:
            self.repo.scm.ignore(self.path)

    def get_files_number(self):
        if not self.use_cache or not self.checksum:
            return 0

        if self.is_dir_checksum:
            return len(self.dir_cache)

        return 1

    def unprotect(self):
        if self.exists:
            self.remote.unprotect(self.path_info)
