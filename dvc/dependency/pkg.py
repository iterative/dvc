from __future__ import unicode_literals

import os
import copy

from dvc.utils.compat import urlparse

from .local import DependencyLOCAL


class DependencyPKG(DependencyLOCAL):
    PARAM_PKG = "pkg"

    def __init__(self, pkg, stage, *args, **kwargs):
        self.pkg = stage.repo.pkg.get_pkg(**pkg)
        super(DependencyLOCAL, self).__init__(stage, *args, **kwargs)

    def _parse_path(self, remote, path):
        out_path = os.path.join(
            self.pkg.repo.root_dir, urlparse(path).path.lstrip("/")
        )

        out, = self.pkg.repo.find_outs_by_path(out_path)
        self.info = copy.copy(out.info)
        self._pkg_stage = copy.copy(out.stage.path)
        return self.REMOTE.path_cls(out.cache_path)

    @property
    def is_in_repo(self):
        return False

    def dumpd(self):
        ret = super(DependencyLOCAL, self).dumpd()
        ret[self.PARAM_PKG] = self.pkg.dumpd()
        return ret

    def download(self, to, resume=False):
        self.pkg.repo.fetch(self._pkg_stage)
        to.info = copy.copy(self.info)
        to.checkout()
