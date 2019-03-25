from __future__ import unicode_literals

from dvc.repo.scm_context import scm_context


@scm_context
def run(
    self,
    cmd=None,
    deps=None,
    outs=None,
    outs_no_cache=None,
    metrics=None,
    metrics_no_cache=None,
    fname=None,
    cwd=None,
    wdir=None,
    no_exec=False,
    overwrite=False,
    ignore_build_cache=False,
    remove_outs=False,
    no_commit=False,
    outs_persist=None,
    outs_persist_no_cache=None,
):
    from dvc.stage import Stage

    if outs is None:
        outs = []
    if deps is None:
        deps = []
    if outs_no_cache is None:
        outs_no_cache = []
    if metrics is None:
        metrics = []
    if metrics_no_cache is None:
        metrics_no_cache = []
    if outs_persist is None:
        outs_persist = []
    if outs_persist_no_cache is None:
        outs_persist_no_cache = []

    with self.state:
        stage = Stage.create(
            repo=self,
            fname=fname,
            cmd=cmd,
            cwd=cwd,
            wdir=wdir,
            outs=outs,
            outs_no_cache=outs_no_cache,
            metrics=metrics,
            metrics_no_cache=metrics_no_cache,
            deps=deps,
            overwrite=overwrite,
            ignore_build_cache=ignore_build_cache,
            remove_outs=remove_outs,
            outs_persist=outs_persist,
            outs_persist_no_cache=outs_persist_no_cache,
        )

    if stage is None:
        return None

    self.check_dag(self.stages() + [stage])

    with self.state:
        if not no_exec:
            stage.run(no_commit=no_commit)

    stage.dump()

    return stage
