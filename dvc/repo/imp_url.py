import os

from . import locked as locked_repo
from dvc.repo.scm_context import scm_context
from dvc.utils import resolve_output, resolve_paths, relpath
from dvc.utils.fs import path_isin


@locked_repo
@scm_context
def imp_url(self, url, out=None, fname=None, erepo=None, locked=True):
    from dvc.dvcfile import Dvcfile
    from dvc.stage import Stage

    out = resolve_output(url, out)
    path, wdir, out = resolve_paths(self, out)

    # NOTE: when user is importing something from within his own repository
    if os.path.exists(url) and path_isin(os.path.abspath(url), self.root_dir):
        url = relpath(url, wdir)

    stage = Stage.create(
        self, fname or path, wdir=wdir, deps=[url], outs=[out], erepo=erepo,
    )

    if stage is None:
        return None

    dvcfile = Dvcfile(self, stage.path)
    dvcfile.overwrite_with_prompt(force=True)

    self.check_modified_graph([stage])

    stage.run()

    stage.locked = locked

    dvcfile.dump(stage)

    return stage
