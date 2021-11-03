import logging
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Dict, Iterable, cast

from funcy import compact, lremove, post_processing
from rich.prompt import Confirm, Prompt
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


class RichInputMixin:
    """Prevents exc message from printing in the same line on Ctrl + D/C."""

    @classmethod
    def get_input(cls, *args, **kwargs) -> str:
        try:
            return super().get_input(*args, **kwargs)  # type: ignore[misc]
        except (KeyboardInterrupt, EOFError):
            ui.error_write()
            raise


class ConfirmationPrompt(RichInputMixin, Confirm):
    def make_prompt(self, default):
        prompt = self.prompt.copy()
        prompt.end = ""

        prompt.append(" [")
        yes, no = self.choices
        for idx, (val, choice) in enumerate(((True, yes), (False, no))):
            if idx:
                prompt.append("/")
            if val == default:
                prompt.append(choice.upper(), "green")
            else:
                prompt.append(choice)
        prompt.append("] ")
        return prompt


class RequiredPrompt(RichInputMixin, Prompt):
    def process_response(self, value: str):
        from rich.prompt import InvalidResponse

        ret = super().process_response(value)
        if not ret:
            raise InvalidResponse(
                "[prompt.invalid]Response required. Please try again."
            )
        return ret

    def render_default(self, default):
        from rich.text import Text

        return Text(f"{default!s}", "green")


class SkippablePrompt(RequiredPrompt):
    skip_value: str = "n"

    def process_response(self, value: str):
        ret = super().process_response(value)
        return None if ret == self.skip_value else ret

    def make_prompt(self, default):
        prompt = self.prompt.copy()
        prompt.end = ""

        prompt.append(" [")
        if (
            default is not ...
            and self.show_default
            and isinstance(default, (str, self.response_type))
        ):
            _default = self.render_default(default)
            prompt.append(_default)
            prompt.append(", ")

        prompt.append(f"{self.skip_value} to omit", style="italic")
        prompt.append("]")
        prompt.append(self.prompt_suffix)
        return prompt


@post_processing(dict)
def _prompts(keys: Iterable[str], defaults: Dict[str, OptStr]):
    for key in keys:
        if key == "cmd":
            prompt_cls = RequiredPrompt
        else:
            prompt_cls = SkippablePrompt
        kwargs = {"default": defaults[key]} if key in defaults else {}
        prompt = PROMPTS[key]
        value = prompt_cls.ask(  # type: ignore[call-overload]
            prompt, console=ui.error_console, **kwargs
        )
        yield key, value


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
        for value in sorted(workspace.values()):
            tree.add(f"[green]{value}[/green]")
        ui.error_write(tree, styled=True)

    ui.error_write()
    ret.update(_prompts(primary, defaults))
    ret.update(_prompts(secondary, defaults))
    return compact(ret)


def _check_stage_exists(
    dvcfile: "DVCFile", name: str, force: bool = False
) -> None:
    if not force and dvcfile.exists() and name in dvcfile.stages:
        from dvc.stage.exceptions import DuplicateStageName

        hint = "Use '--force' to overwrite."
        raise DuplicateStageName(
            f"Stage '{name}' already exists in 'dvc.yaml'. {hint}"
        )


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
    if interactive:
        defaults = init_interactive(
            name,
            defaults=defaults,
            live=with_live,
            provided=overrides,
            show_tree=True,
        )
    else:
        if with_live:
            # suppress `metrics`/`params` if live is selected, unless
            # it is also provided via overrides/cli.
            # This makes output to be a checkpoint as well.
            defaults.pop("metrics", None)
            defaults.pop("params", None)
        else:
            defaults.pop("live", None)  # suppress live otherwise

    context: Dict[str, str] = {**defaults, **overrides}
    assert "cmd" in context

    params_kv = []
    params = context.get("params")
    if params:
        from dvc.utils.serialize import LOADERS

        assert isinstance(params, str)
        _, ext = os.path.splitext(params)
        params_kv = [{params: list(LOADERS[ext](params))}]

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

    if not interactive or ConfirmationPrompt.ask(
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
