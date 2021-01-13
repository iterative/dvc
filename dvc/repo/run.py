import os
from contextlib import suppress

from funcy import concat, first, without

from dvc.utils import chunk_dict
from dvc.utils.cli_parse import parse_params_from_cli

from . import locked
from .scm_context import scm_context


def is_valid_name(name: str):
    from ..stage import INVALID_STAGENAME_CHARS

    return not INVALID_STAGENAME_CHARS & set(name)


def _get_file_path(kwargs):
    from dvc.dvcfile import DVC_FILE, DVC_FILE_SUFFIX

    out = first(
        concat(
            kwargs.get("outs", []),
            kwargs.get("outs_no_cache", []),
            kwargs.get("metrics", []),
            kwargs.get("metrics_no_cache", []),
            kwargs.get("plots", []),
            kwargs.get("plots_no_cache", []),
            kwargs.get("outs_persist", []),
            kwargs.get("outs_persist_no_cache", []),
            kwargs.get("checkpoints", []),
            without([kwargs.get("live", None)], None),
        )
    )

    return (
        os.path.basename(os.path.normpath(out)) + DVC_FILE_SUFFIX
        if out
        else DVC_FILE
    )


def _check_stage_exists(dvcfile, stage):
    from dvc.stage import PipelineStage
    from dvc.stage.exceptions import (
        DuplicateStageName,
        StageFileAlreadyExistsError,
    )

    if not dvcfile.exists():
        return

    hint = "Use '--force' to overwrite."
    if stage.__class__ != PipelineStage:
        raise StageFileAlreadyExistsError(
            f"'{stage.relpath}' already exists. {hint}"
        )
    elif stage.name and stage.name in dvcfile.stages:
        raise DuplicateStageName(
            f"Stage '{stage.name}' already exists in '{stage.relpath}'. {hint}"
        )


@locked
@scm_context
def run(self, fname=None, no_exec=False, single_stage=False, **kwargs):
    from dvc.dvcfile import PIPELINE_FILE, Dvcfile
    from dvc.exceptions import InvalidArgumentError, OutputDuplicationError
    from dvc.stage import PipelineStage, Stage, create_stage, restore_meta
    from dvc.stage.exceptions import InvalidStageName

    if not kwargs.get("cmd"):
        raise InvalidArgumentError("command is not specified")

    stage_cls = PipelineStage
    path = PIPELINE_FILE
    stage_name = kwargs.get("name")

    if stage_name and single_stage:
        raise InvalidArgumentError(
            "`-n|--name` is incompatible with `--single-stage`"
        )

    if stage_name and fname:
        raise InvalidArgumentError(
            "`--file` is currently incompatible with `-n|--name` "
            "and requires `--single-stage`"
        )

    if not stage_name and not single_stage:
        raise InvalidArgumentError("`-n|--name` is required")

    if single_stage:
        kwargs.pop("name", None)
        stage_cls = Stage
        path = fname or _get_file_path(kwargs)
    else:
        if not is_valid_name(stage_name):
            raise InvalidStageName

    params = chunk_dict(parse_params_from_cli(kwargs.pop("params", [])))
    stage = create_stage(
        stage_cls, repo=self, path=path, params=params, **kwargs
    )
    restore_meta(stage)
    if kwargs.get("run_cache", True) and stage.can_be_skipped:
        return None

    dvcfile = Dvcfile(self, stage.path)
    try:
        if kwargs.get("force", True):
            with suppress(ValueError):
                self.stages.remove(stage)
        else:
            _check_stage_exists(dvcfile, stage)
        self.check_modified_graph([stage])
    except OutputDuplicationError as exc:
        raise OutputDuplicationError(exc.output, set(exc.stages) - {stage})

    if no_exec:
        stage.ignore_outs()
    else:
        stage.run(
            no_commit=kwargs.get("no_commit", False),
            run_cache=kwargs.get("run_cache", True),
        )

    dvcfile.dump(stage, update_lock=not no_exec)
    return stage
