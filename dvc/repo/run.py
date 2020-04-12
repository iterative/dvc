import os

from . import locked
from .scm_context import scm_context


@locked
@scm_context
def run(self, fname=None, no_exec=False, **kwargs):
    from dvc.stage import Stage

    outs = (
        kwargs.get("outs", [])
        + kwargs.get("outs_no_cache", [])
        + kwargs.get("metrics", [])
        + kwargs.get("metrics_no_cache", [])
        + kwargs.get("outs_persist", [])
        + kwargs.get("outs_persist_no_cache", [])
    )

    if outs:
        base = os.path.basename(os.path.normpath(outs[0]))
        path = base + Stage.STAGE_FILE_SUFFIX
    else:
        path = Stage.STAGE_FILE

    stage = Stage.create(self, fname or path, **kwargs)
    if stage is None:
        return None

    self.check_modified_graph([stage])

    if not no_exec:
        stage.run(no_commit=kwargs.get("no_commit", False))

    stage.dump()

    return stage
