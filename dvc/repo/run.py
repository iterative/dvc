import os

from . import locked
from .scm_context import scm_context
from dvc.stage.exceptions import DuplicateStageName, InvalidStageName

from funcy import first, concat

from ..exceptions import OutputDuplicationError


def is_valid_name(name: str):
    return not {"\\", "/", "@", ":"} & set(name)


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
    from dvc.dvcfile import Dvcfile, PIPELINE_FILE

    stage_cls = PipelineStage
    path = PIPELINE_FILE
    stage_name = kwargs.get("name")
    if not stage_name:
        kwargs.pop("name", None)
        stage_cls = Stage
        path = fname or _get_file_path(kwargs)
    else:
        if not is_valid_name(stage_name):
            raise InvalidStageName

    stage = create_stage(stage_cls, repo=self, path=path, **kwargs)
    if stage is None:
        return None

    dvcfile = Dvcfile(self, stage.path)
    if dvcfile.exists():
        if stage_name and stage_name in dvcfile.stages:
            raise DuplicateStageName(stage_name, dvcfile)
        if stage_cls != PipelineStage:
            dvcfile.remove_with_prompt(force=kwargs.get("overwrite", True))

    try:
        self.check_modified_graph([stage])
    except OutputDuplicationError as exc:
        raise OutputDuplicationError(exc.output, set(exc.stages) - {stage})

    if not no_exec:
        stage.run(no_commit=kwargs.get("no_commit", False))
    dvcfile.dump(stage, update_pipeline=True)
    return stage
