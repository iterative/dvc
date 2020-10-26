import logging
import os
from urllib.parse import urlparse

from dvc.exceptions import DvcException
from dvc.istextfile import istextfile
from dvc.output.base import BaseOutput
from dvc.utils import relpath
from dvc.utils.fs import path_isin

from ..tree.local import LocalTree

logger = logging.getLogger(__name__)


class LocalOutput(BaseOutput):
    TREE_CLS = LocalTree
    sep = os.sep

    def __init__(self, stage, path, *args, **kwargs):
        if stage and path_isin(path, stage.repo.root_dir):
            path = relpath(path, stage.wdir)

        super().__init__(stage, path, *args, **kwargs)
        if (
            self.is_in_repo
            and self.repo
            and isinstance(self.repo.tree, LocalTree)
        ):
            self.tree = self.repo.tree

    def _parse_path(self, tree, path):
        parsed = urlparse(path)
        if parsed.scheme == "remote":
            p = tree.path_info / parsed.path.lstrip("/")
        else:
            # NOTE: we can path either from command line or .dvc file,
            # so we should expect both posix and windows style paths.
            # PathInfo accepts both, i.e. / works everywhere, \ only on win.
            #
            # FIXME: if we have Windows path containing / or posix one with \
            # then we have #2059 bug and can't really handle that.
            p = self.TREE_CLS.PATH_CLS(path)
            if self.stage and not p.is_absolute():
                p = self.stage.wdir / p

        abs_p = os.path.abspath(os.path.normpath(p))
        return self.TREE_CLS.PATH_CLS(abs_p)

    def __str__(self):
        if not self.repo or not self.is_in_repo:
            return str(self.def_path)

        cur_dir = os.getcwd()
        if path_isin(cur_dir, self.repo.root_dir):
            return relpath(self.path_info, cur_dir)

        return relpath(self.path_info, self.repo.root_dir)

    @property
    def fspath(self):
        return self.path_info.fspath

    @property
    def is_in_repo(self):
        def_scheme = urlparse(self.def_path).scheme
        return def_scheme != "remote" and not os.path.isabs(self.def_path)

    def dumpd(self):
        ret = super().dumpd()
        if self.is_in_repo:
            path = self.path_info.relpath(self.stage.wdir).as_posix()
        else:
            path = self.def_path

        ret[self.PARAM_PATH] = path

        return ret

    def verify_metric(self):
        if not self.metric or self.plot:
            return

        path = os.fspath(self.path_info)
        if not os.path.exists(path):
            return

        name = "metrics" if self.metric else "plot"
        if os.path.isdir(path):
            msg = "directory '%s' cannot be used as %s."
            logger.debug(msg, str(self.path_info), name)
            return

        if not istextfile(path):
            msg = "binary file '{}' cannot be used as {}."
            raise DvcException(msg.format(self.path_info, name))
