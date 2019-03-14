from __future__ import unicode_literals

import os

import dvc.logger as logger
from dvc.utils.compat import urlparse
from dvc.istextfile import istextfile
from dvc.exceptions import DvcException
from dvc.remote.local import RemoteLOCAL
from dvc.output.base import OutputBase, OutputAlreadyTrackedError


class OutputLOCAL(OutputBase):
    REMOTE = RemoteLOCAL

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
        super(OutputLOCAL, self).__init__(
            stage,
            path,
            info,
            remote=remote,
            cache=cache,
            metric=metric,
            persist=persist,
            tags=tags,
        )
        if remote:
            p = os.path.join(
                remote.prefix, urlparse(self.url).path.lstrip("/")
            )
        else:
            p = path

        if not os.path.isabs(p):
            p = self.remote.to_ospath(p)
            p = os.path.join(stage.wdir, p)
        p = os.path.abspath(os.path.normpath(p))

        self.path_info = {"scheme": "local", "path": p}

        self._dir_cache = {}

    def __str__(self):
        return self.rel_path

    @property
    def is_local(self):
        return urlparse(self.url).scheme != "remote" and not os.path.isabs(
            self.url
        )

    def assign_to_stage_file(self, stage):
        from dvc.repo import Repo

        fullpath = os.path.abspath(stage.wdir)
        self.path_info["path"] = os.path.join(fullpath, self.stage_path)

        self.repo = Repo(self.path)

        self.stage = stage
        return self

    @property
    def sep(self):
        return os.sep

    @property
    def rel_path(self):
        return os.path.relpath(self.path)

    @property
    def stage_path(self):
        return os.path.relpath(self.path, self.stage.wdir)

    @property
    def cache(self):
        return self.repo.cache.local.get(self.checksum)

    @property
    def is_dir_cache(self):
        return self.repo.cache.local.is_dir_cache(self.checksum)

    def dumpd(self):
        ret = super(OutputLOCAL, self).dumpd()
        if self.is_local:
            path = self.remote.unixpath(
                os.path.relpath(self.path, self.stage.wdir)
            )
        else:
            path = self.url

        ret[self.PARAM_PATH] = path

        return ret

    def verify_metric(self):
        if not self.metric:
            return

        if not os.path.exists(self.path):
            return

        if os.path.isdir(self.path):
            msg = "directory '{}' cannot be used as metrics."
            raise DvcException(msg.format(self.rel_path))

        if not istextfile(self.path):
            msg = "binary file '{}' cannot be used as metrics."
            raise DvcException(msg.format(self.rel_path))

    def save(self):
        if not os.path.exists(self.path):
            raise self.DoesNotExistError(self.rel_path)

        if not os.path.isfile(self.path) and not os.path.isdir(self.path):
            raise self.IsNotFileOrDirError(self.rel_path)

        if (os.path.isfile(self.path) and os.path.getsize(self.path) == 0) or (
            os.path.isdir(self.path) and len(os.listdir(self.path)) == 0
        ):
            msg = "file/directory '{}' is empty.".format(self.rel_path)
            logger.warning(msg)

        if not self.use_cache:
            self.info = self.remote.save_info(self.path_info)
            self.verify_metric()
            if not self.IS_DEPENDENCY:
                msg = "Output '{}' doesn't use cache. Skipping saving."
                logger.info(msg.format(self.rel_path))
            return

        assert not self.IS_DEPENDENCY

        if not self.changed():
            msg = "Output '{}' didn't change. Skipping saving."
            logger.info(msg.format(self.rel_path))
            return

        if self.is_local:
            if self.repo.scm.is_tracked(self.path):
                raise OutputAlreadyTrackedError(self.rel_path)

            if self.use_cache:
                self.repo.scm.ignore(self.path)

        self.info = self.remote.save_info(self.path_info)

    @property
    def dir_cache(self):
        return self.repo.cache.local.load_dir_cache(self.checksum)

    def get_files_number(self):
        if self.cache is None:
            return 0

        if self.is_dir_cache:
            return len(self.dir_cache)
        return 1
