import os

from dvc.repo.scm_context import scm_context
from dvc.utils import relpath, resolve_output, resolve_paths
from dvc.utils.fs import path_isin

from ..exceptions import OutputDuplicationError
from . import locked


@locked
@scm_context
def imp_url(
    self,
    url,
    out=None,
    fname=None,
    erepo=None,
    frozen=True,
    no_exec=False,
    remote=None,
    track_remote_url=True,
    straight_to_remote=False,
    desc=None,
    jobs=None,
    command="import-url",
):
    from dvc.dvcfile import Dvcfile
    from dvc.stage import Stage, create_stage, restore_meta

    out = resolve_output(url, out)
    path, wdir, out = resolve_paths(self, out)

    # NOTE: when user is importing something from within their own repository
    if (
        erepo is None
        and os.path.exists(url)
        and path_isin(os.path.abspath(url), self.root_dir)
    ):
        url = relpath(url, wdir)

    deps = [url]
    if not track_remote_url:
        deps.clear()

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

    if not straight_to_remote:
        try:
            self.check_modified_graph([stage])
        except OutputDuplicationError as exc:
            raise OutputDuplicationError(exc.output, set(exc.stages) - {stage})

    if no_exec:
        stage.ignore_outs()
    elif straight_to_remote:
        stage.outs[0].hash_info = self.cloud.transfer(
            url, remote=remote, command=command
        )
    else:
        stage.run(jobs=jobs)

    stage.frozen = frozen

    dvcfile.dump(stage)

    return stage
