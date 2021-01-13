from typing import TYPE_CHECKING

from dvc.exceptions import InvalidArgumentError

from . import locked
from .scm_context import scm_context

if TYPE_CHECKING:
    from . import Repo


@locked
@scm_context
def run(
    self: "Repo",
    fname: str = None,
    no_exec: bool = False,
    single_stage: bool = False,
    **kwargs
):
    from dvc.stage.utils import check_graphs, create_stage_from_cli

    if not kwargs.get("cmd"):
        raise InvalidArgumentError("command is not specified")

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

    stage = create_stage_from_cli(
        self, single_stage=single_stage, fname=fname, **kwargs
    )
    if kwargs.get("run_cache", True) and stage.can_be_skipped:
        return None

    check_graphs(self, stage, force=kwargs.get("force", True))

    if no_exec:
        stage.ignore_outs()
    else:
        stage.run(
            no_commit=kwargs.get("no_commit", False),
            run_cache=kwargs.get("run_cache", True),
        )

    stage.dump(update_lock=not no_exec)
    return stage
