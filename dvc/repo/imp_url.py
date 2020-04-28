import os

from . import locked as locked_repo
from dvc.repo.scm_context import scm_context
from dvc.utils import resolve_output, resolve_paths, relpath
from dvc.utils.fs import path_isin
from ..exceptions import OutputDuplicationError


@locked_repo
@scm_context
def imp_url(self, url, out=None, fname=None, erepo=None, locked=True):
    from dvc.dvcfile import Dvcfile
    from dvc.stage import Stage, create_stage

    out = resolve_output(url, out)
    path, wdir, out = resolve_paths(self, out)

    # NOTE: when user is importing something from within his own repository
    if os.path.exists(url) and path_isin(os.path.abspath(url), self.root_dir):
        url = relpath(url, wdir)

    stage = create_stage(
        Stage,
        self,
        fname or path,
        wdir=wdir,
        deps=[url],
        outs=[out],
        erepo=erepo,
    )

    if stage is None:
        return None

    dvcfile = Dvcfile(self, stage.path)
    dvcfile.remove_with_prompt(force=True)

    try:
        self.check_modified_graph([stage])
    except OutputDuplicationError as exc:
        raise OutputDuplicationError(exc.output, set(exc.stages) - {stage})

    stage.run()

    stage.locked = locked

    dvcfile.dump(stage)

    return stage
