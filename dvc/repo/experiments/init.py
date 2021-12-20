import logging
import os
from contextlib import contextmanager
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    TextIO,
    Tuple,
    Union,
    cast,
)

from funcy import compact, lremove
from rich.rule import Rule
from rich.syntax import Syntax

from dvc.exceptions import DvcException
from dvc.stage import PipelineStage
from dvc.stage.serialize import to_pipeline_file
from dvc.types import OptStr
from dvc.utils.serialize import dumps_yaml

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.dvcfile import DVCFile
    from rich.tree import Tree

from dvc.ui import ui

PROMPTS = {
    "cmd": "[b]Command[/b] to execute",
    "code": "Path to a [b]code[/b] file/directory",
    "data": "Path to a [b]data[/b] file/directory",
    "models": "Path to a [b]model[/b] file/directory",
    "params": "Path to a [b]parameters[/b] file",
    "metrics": "Path to a [b]metrics[/b] file",
    "plots": "Path to a [b]plots[/b] file/directory",
    "live": "Path to log [b]dvclive[/b] outputs",
}


def _prompts(
    keys: Iterable[str],
    defaults: Dict[str, str] = None,
    validator: Callable[[str, str], Union[str, Tuple[str, str]]] = None,
    allow_omission: bool = True,
    stream: Optional[TextIO] = None,
) -> Dict[str, OptStr]:
    from dvc.ui.prompt import Prompt

    defaults = defaults or {}
    return {
        key: Prompt.prompt_(
            PROMPTS[key],
            console=ui.error_console,
            default=defaults.get(key),
            validator=partial(validator, key) if validator else None,
            allow_omission=allow_omission,
            stream=stream,
        )
        for key in keys
    }


@contextmanager
def _disable_logging(highest_level=logging.CRITICAL):
    previous_level = logging.root.manager.disable

    logging.disable(highest_level)

    try:
        yield
    finally:
        logging.disable(previous_level)


def build_workspace_tree(workspace: Dict[str, str]) -> "Tree":
    from rich.tree import Tree

    tree = Tree(
        "DVC assumes the following workspace structure:",
        highlight=True,
    )
    for value in sorted(workspace.values()):
        tree.add(f"[green]{value}[/green]")
    return tree


PIPELINE_FILE_LINK = "https://s.dvc.org/g/pipeline-files"


def init_interactive(
    name: str,
    defaults: Dict[str, str],
    provided: Dict[str, str],
    validator: Callable[[str, str], Union[str, Tuple[str, str]]] = None,
    live: bool = False,
    stream: Optional[TextIO] = None,
) -> Dict[str, str]:
    command = provided.pop("cmd", None)
    primary = lremove(provided.keys(), ["code", "data", "models", "params"])
    secondary = lremove(
        provided.keys(), ["live"] if live else ["metrics", "plots"]
    )
    prompts = primary + secondary

    workspace = {**defaults, **provided}
    if not live and "live" not in provided:
        workspace.pop("live", None)
    for key in ("plots", "metrics"):
        if live and key not in provided:
            workspace.pop(key, None)

    ret: Dict[str, str] = {}
    if command:
        ret["cmd"] = command

    if not prompts and command:
        return ret

    ui.error_write(
        f"This command will guide you to set up a [bright_blue]{name}[/]",
        "stage in [green]dvc.yaml[/].",
        f"\nSee [repr.url]{PIPELINE_FILE_LINK}[/].\n",
        styled=True,
    )

    if not command:
        ret.update(
            compact(_prompts(["cmd"], allow_omission=False, stream=stream))
        )
        if prompts:
            ui.error_write(styled=True)

    if not prompts:
        return ret

    ui.error_write(
        "Enter the paths for dependencies and outputs of the command.",
        styled=True,
    )
    if workspace:
        ui.error_write(build_workspace_tree(workspace), styled=True)
    ui.error_write(styled=True)
    ret.update(
        compact(
            _prompts(prompts, defaults, validator=validator, stream=stream)
        )
    )
    return ret


def _check_stage_exists(
    dvcfile: "DVCFile", name: str, force: bool = False
) -> None:
    if not force and dvcfile.exists() and name in dvcfile.stages:
        from dvc.stage.exceptions import DuplicateStageName

        hint = "Use '--force' to overwrite."
        raise DuplicateStageName(
            f"Stage '{name}' already exists in 'dvc.yaml'. {hint}"
        )


def loadd_params(path: str) -> Dict[str, List[str]]:
    from dvc.utils.serialize import LOADERS

    _, ext = os.path.splitext(path)
    return {path: list(LOADERS[ext](path))}


def validate_prompts(key: str, value: str) -> Union[Any, Tuple[Any, str]]:
    from dvc.ui.prompt import InvalidResponse

    if key == "params":
        assert isinstance(value, str)
        msg_format = (
            "[prompt.invalid]'{0}' {1}. "
            "Please retry with an existing parameters file."
        )
        if not os.path.exists(value):
            raise InvalidResponse(msg_format.format(value, "does not exist"))
        if os.path.isdir(value):
            raise InvalidResponse(msg_format.format(value, "is a directory"))
    elif key in ("code", "data"):
        if not os.path.exists(value):
            return value, (
                f"[yellow]'{value}' does not exist in the workspace. "
                '"exp run" may fail.[/]'
            )
    return value


def init(
    repo: "Repo",
    name: str = "train",
    type: str = "default",  # pylint: disable=redefined-builtin
    defaults: Dict[str, str] = None,
    overrides: Dict[str, str] = None,
    interactive: bool = False,
    force: bool = False,
    stream: Optional[TextIO] = None,
) -> PipelineStage:
    from dvc.dvcfile import make_dvcfile

    dvcfile = make_dvcfile(repo, "dvc.yaml")
    _check_stage_exists(dvcfile, name, force=force)

    defaults = defaults.copy() if defaults else {}
    overrides = overrides.copy() if overrides else {}

    with_live = type == "dl"

    if interactive:
        defaults = init_interactive(
            name,
            validator=validate_prompts,
            defaults=defaults,
            live=with_live,
            provided=overrides,
            stream=stream,
        )
    else:
        if with_live:
            # suppress `metrics`/`plots` if live is selected, unless
            # it is also provided via overrides/cli.
            # This makes output to be a checkpoint as well.
            defaults.pop("metrics", None)
            defaults.pop("plots", None)
        else:
            defaults.pop("live", None)  # suppress live otherwise

    context: Dict[str, str] = {**defaults, **overrides}
    assert "cmd" in context

    params_kv = []
    params = context.get("params")
    if params:
        params_kv.append(loadd_params(params))

    checkpoint_out = bool(context.get("live"))
    models = context.get("models")
    stage = repo.stage.create(
        name=name,
        cmd=context["cmd"],
        deps=compact([context.get("code"), context.get("data")]),
        params=params_kv,
        metrics_no_cache=compact([context.get("metrics")]),
        plots_no_cache=compact([context.get("plots")]),
        live=context.get("live"),
        force=force,
        **{"checkpoints" if checkpoint_out else "outs": compact([models])},
    )

    if interactive:
        ui.error_write(Rule(style="green"), styled=True)
        _yaml = dumps_yaml(to_pipeline_file(cast(PipelineStage, stage)))
        syn = Syntax(_yaml, "yaml", theme="ansi_dark")
        ui.error_write(syn, styled=True)

    from dvc.ui.prompt import Confirm

    if not interactive or Confirm.ask(
        "Do you want to add the above contents to dvc.yaml?",
        console=ui.error_console,
        default=True,
        stream=stream,
    ):
        with _disable_logging(), repo.scm_context(autostage=True, quiet=True):
            stage.dump(update_lock=False)
            stage.ignore_outs()
            if params:
                repo.scm_context.track_file(params)
    else:
        raise DvcException("Aborting ...")

    assert isinstance(stage, PipelineStage)
    return stage
