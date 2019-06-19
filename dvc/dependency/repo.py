from __future__ import unicode_literals

import os
import copy

from dvc.utils.compat import urlparse
from dvc.external_repo import ExternalRepo

from .local import DependencyLOCAL


class DependencyREPO(DependencyLOCAL):
    PARAM_REPO = "repo"

    def __init__(self, erepo, stage, *args, **kwargs):
        self.erepo = ExternalRepo(stage.repo.dvc_dir, **erepo)
        super(DependencyLOCAL, self).__init__(stage, *args, **kwargs)

    def _parse_path(self, remote, path):
        self.erepo.install(self.repo.cache.local.cache_dir)

        out_path = os.path.join(
            self.erepo.repo.root_dir, urlparse(path).path.lstrip("/")
        )

        out, = self.erepo.repo.find_outs_by_path(out_path)
        self.info = copy.copy(out.info)
        self._erepo_stage = copy.copy(out.stage.path)
        return self.REMOTE.path_cls(out.cache_path)

    @property
    def is_in_repo(self):
        return False

    def dumpd(self):
        ret = super(DependencyLOCAL, self).dumpd()
        ret[self.PARAM_REPO] = self.erepo.dumpd()
        return ret

    def download(self, to, resume=False):
        self.erepo.repo.fetch(self._erepo_stage)
        to.info = copy.copy(self.info)
        to.checkout()
