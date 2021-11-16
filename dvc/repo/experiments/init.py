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
from dvc.utils.serialize import dumps_yaml

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.dvcfile import DVCFile
    from dvc.stage import Stage

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
    defaults: Dict[str, str],
    validator: Callable[[str, str], Union[str, Tuple[str, str]]] = None,
) -> Dict[str, str]:
    from dvc.ui.prompt import Prompt

    ret: Dict[str, str] = {}
    for key in keys:
        value = Prompt.prompt_(
            PROMPTS[key],
            console=ui.error_console,
            default=defaults.get(key),
            validator=partial(validator, key) if validator else None,
            allow_omission=key != "cmd",
        )
        if value:
            ret[key] = value
    return ret


@contextmanager
def _disable_logging(highest_level=logging.CRITICAL):
    previous_level = logging.root.manager.disable

    logging.disable(highest_level)

    try:
        yield
    finally:
        logging.disable(previous_level)


PIPELINE_FILE_LINK = (
    "https://dvc.org/doc/user-guide/project-structure/pipelines-files"
)


def init_interactive(
    name: str,
    defaults: Dict[str, str],
    provided: Dict[str, str],
    validator: Callable[[str, str], Union[str, Tuple[str, str]]] = None,
    show_tree: bool = False,
    live: bool = False,
) -> Dict[str, str]:
    command = provided.pop("cmd", None)
    primary = lremove(provided.keys(), ["code", "data", "models", "params"])
    secondary = lremove(
        provided.keys(), ["live"] if live else ["metrics", "plots"]
    )

    ret: Dict[str, str] = {}
    if not (command or primary or secondary):
        return ret

    message = ui.rich_text.assemble(
        "This command will guide you to set up a ",
        (name, "bright_blue"),
        " stage in ",
        ("dvc.yaml", "green"),
        ".",
    )
    doc_link = ui.rich_text.assemble(
        "See ", (PIPELINE_FILE_LINK, "repr.url"), "."
    )
    ui.error_write(message, doc_link, "", sep="\n", styled=True)

    if not command:
        ret.update(_prompts(["cmd"], defaults))
        ui.write()
    else:
        ret.update({"cmd": command})

    ui.write("Enter the paths for dependencies and outputs of the command.")
    workspace = {**defaults, **provided}
    if show_tree and workspace:
        from rich.tree import Tree

        tree = Tree(
            "DVC assumes the following workspace structure:",
            highlight=True,
        )
        if not live and "live" not in provided:
            workspace.pop("live", None)
        for key in ("plots", "metrics"):
            if live and key not in provided:
                workspace.pop(key, None)
        for value in sorted(workspace.values()):
            tree.add(f"[green]{value}[/green]")
        ui.error_write(tree, styled=True)

    ui.error_write()
    ret.update(_prompts(primary, defaults, validator=validator))
    ret.update(_prompts(secondary, defaults, validator=validator))
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


def init(
    repo: "Repo",
    name: str = None,
    type: str = "default",  # pylint: disable=redefined-builtin
    defaults: Dict[str, str] = None,
    overrides: Dict[str, str] = None,
    interactive: bool = False,
    force: bool = False,
) -> "Stage":
    from dvc.dvcfile import make_dvcfile

    dvcfile = make_dvcfile(repo, "dvc.yaml")
    name = name or type

    _check_stage_exists(dvcfile, name, force=force)

    defaults = defaults or {}
    overrides = overrides or {}

    with_live = type == "live"

    def validate_prompts_input(
        key: str, value: str
    ) -> Union[Any, Tuple[Any, str]]:
        from dvc.ui.prompt import InvalidResponse

        if key == "params":
            assert isinstance(value, str)
            try:
                loadd_params(value)
            except (FileNotFoundError, IsADirectoryError) as exc:
                reason = "does not exist"
                if isinstance(exc, IsADirectoryError):
                    reason = "is a directory"
                raise InvalidResponse(
                    f"[prompt.invalid]'{value}' {reason}. "
                    "Please retry with an existing parameters file."
                )
        elif key in ("code", "data"):
            if not os.path.exists(value):
                return value, (
                    f"[yellow]'{value}' does not exist in the workspace. "
                    '"exp run" may fail.[/]'
                )
        return value

    if interactive:
        defaults = init_interactive(
            name,
            validator=validate_prompts_input,
            defaults=defaults,
            live=with_live,
            provided=overrides,
            show_tree=True,
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
        ui.write(Rule(style="green"), styled=True)
        _yaml = dumps_yaml(to_pipeline_file(cast(PipelineStage, stage)))
        syn = Syntax(_yaml, "yaml", theme="ansi_dark")
        ui.error_write(syn, styled=True)

    from dvc.ui.prompt import Confirm

    if not interactive or Confirm.ask(
        "Do you want to add the above contents to dvc.yaml?",
        console=ui.error_console,
        default=True,
    ):
        scm = repo.scm
        with _disable_logging(), scm.track_file_changes(autostage=True):
            stage.dump(update_lock=False)
            stage.ignore_outs()
            if params:
                scm.track_file(params)
    else:
        raise DvcException("Aborting ...")
    return stage
