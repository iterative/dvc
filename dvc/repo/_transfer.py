import os

from dvc.repo.scm_context import scm_context
from dvc.utils import relpath, resolve_output, resolve_paths
from dvc.utils.fs import path_isin

from ..exceptions import OutputDuplicationError
from . import locked


@locked
@scm_context
def _transfer(
    self,
    url,
    command,
    out=None,
    fname=None,
    erepo=None,
    frozen=True,
    remote=None,
    desc=None,
    jobs=None,
):
    from dvc.dvcfile import Dvcfile
    from dvc.stage import Stage, create_stage, restore_meta

    out = resolve_output(url, out)
    path, wdir, out = resolve_paths(self, out)

    # NOTE: when user is transfering something from within their own repository
    if (
        erepo is None
        and os.path.exists(url)
        and path_isin(os.path.abspath(url), self.root_dir)
    ):
        url = relpath(url, wdir)

    deps = []
    if command == "import-url":
        deps.append(url)

    stage = create_stage(
        Stage,
        self,
        fname or path,
        wdir=wdir,
        deps=deps,
        outs=[out],
        erepo=erepo,
    )
    restore_meta(stage)

    if stage.can_be_skipped:
        return None
    if desc:
        stage.outs[0].desc = desc

    dvcfile = Dvcfile(self, stage.path)
    dvcfile.remove()

    try:
        self.check_modified_graph([stage])
    except OutputDuplicationError as exc:
        raise OutputDuplicationError(exc.output, set(exc.stages) - {stage})

    stage.frozen = frozen
    stage.outs[0].hash_info = self.cloud.transfer(
        url, jobs=jobs, remote=remote, command=command
    )

    dvcfile.dump(stage)
    return stage
