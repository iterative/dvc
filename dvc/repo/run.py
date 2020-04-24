import os

from . import locked
from .scm_context import scm_context

from funcy import first, concat


def _get_file_path(kwargs):
    from dvc.dvcfile import DVC_FILE_SUFFIX, DVC_FILE

    out = first(
        concat(
            kwargs.get("outs", []),
            kwargs.get("outs_no_cache", []),
            kwargs.get("metrics", []),
            kwargs.get("metrics_no_cache", []),
            kwargs.get("outs_persist", []),
            kwargs.get("outs_persist_no_cache", []),
        )
    )

    return (
        os.path.basename(os.path.normpath(out)) + DVC_FILE_SUFFIX
        if out
        else DVC_FILE
    )


@locked
@scm_context
def run(self, fname=None, no_exec=False, **kwargs):
    from dvc.stage import PipelineStage, Stage, create_stage
    from dvc.dvcfile import Dvcfile

    stage_cls, path = PipelineStage, "pipelines.yaml"
    if not kwargs.get("name"):
        kwargs.pop("name", None)
        stage_cls, path = Stage, fname or _get_file_path(kwargs)

    stage = create_stage(stage_cls, repo=self, path=path, **kwargs)
    if stage is None:
        return None

    dvcfile = Dvcfile(self, stage.path)
    if stage_cls != PipelineStage and dvcfile.exists():
        dvcfile.remove_with_prompt(force=kwargs.get("overwrite", True))

    self.check_modified_graph([stage])
    if not no_exec:
        stage.run(no_commit=kwargs.get("no_commit", False))
    dvcfile.dump(stage, update_pipeline=True)
    return stage
