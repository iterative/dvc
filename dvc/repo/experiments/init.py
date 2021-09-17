import dataclasses
import os
from collections import ChainMap
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
)

from funcy import compact, post_processing
from rich.prompt import Prompt as _Prompt
from voluptuous import MultipleInvalid

from dvc.exceptions import DvcException
from dvc.ui import ui

if TYPE_CHECKING:
    from jinja2 import BaseLoader, Environment, Template

    from dvc.repo import Repo


DEFAULT_TEMPLATE = "default"
STAGES_DIR = Path(__file__).parents[3] / "resources" / "stages"


@dataclasses.dataclass
class TemplateDefaults:
    code: str = "src"
    data: str = "data"
    models: str = "models"
    metrics: str = "metrics.json"
    params: str = "params.yaml"
    plots: str = "plots"
    live: str = "dvclive"


DEFAULT_VALUES = dataclasses.asdict(TemplateDefaults())


def get_loader(repo: "Repo") -> "BaseLoader":
    from jinja2 import ChoiceLoader, FileSystemLoader

    return ChoiceLoader(
        [
            # not initialized yet
            FileSystemLoader(Path(repo.dvc_dir) / "stages"),
            # won't work for other packages
            FileSystemLoader(STAGES_DIR),
        ]
    )


class Prompt(_Prompt):
    def __init__(self, *args, **kwargs):
        self.default = kwargs.pop("default", False)
        super().__init__(*args, **kwargs)

    def process_response(self, value: str):
        from rich.prompt import InvalidResponse

        ret = super().process_response(value)
        if not ret and self.default is None:
            raise InvalidResponse(
                "[prompt.invalid]Response required. Please try again."
            )
        return ret

    def render_default(self, default):
        from rich.text import Text

        return Text(f"({default})", style="bold")

    def __call__(self, *args, **kwargs):
        if self.default is not None:
            kwargs.setdefault("default", self.default)
        return super().__call__(*args, **kwargs)


@post_processing(dict)
def init_interactive(keys, defaults=None):
    defaults = defaults or {}
    prompts = {
        "cmd": "Enter command to run",
        "code": "Enter path to a code file/directory",
        "data": "Enter path to a data file/directory",
        "models": "Enter path to a model file/directory",
        "metrics": "Enter path to a metrics file",
        "params": "Enter path to a parameters file",
        "plots": "Enter path to a plots file/directory",
        "live": "Enter path to log dvclive outputs",
    }
    for key, msg in prompts.items():
        if key not in keys:
            continue

        default = defaults.get(key)
        prompter = Prompt(msg, console=ui.error_console, default=default)
        yield key, prompter()


class Jinja2StaticAnalyzer:
    def __init__(self, environment: "Environment", template: "Template"):
        self.environment = environment
        self.template = template
        self.known_variables = ["cmd"] + list(DEFAULT_VALUES)

    def find_undeclared_variables(
        self, context: Mapping[str, Any] = None
    ) -> Set[str]:
        from jinja2.meta import find_undeclared_variables

        context = context or {}
        rendered = self.template.render(**context)
        ast = self.environment.parse(rendered)
        return find_undeclared_variables(ast)

    def get_undefined_keys(
        self,
        context=None,
        additional_known_keys: List[str] = None,
        ignore_variables: List[str] = None,
    ):
        additional_known_keys = additional_known_keys or []
        known_variables: List[str] = (
            self.known_variables + additional_known_keys
        )
        ignore_variables = ignore_variables or []

        undeclared_variables = self.find_undeclared_variables(context)
        unknown_variables = (
            undeclared_variables - set(ignore_variables) - set(known_variables)
        )
        if unknown_variables:
            raise DvcException(
                f"template '{self.template.name}' has unknown variables: "
                f"{unknown_variables}"
            )
        return list(undeclared_variables - unknown_variables)


def init_template(repo):
    from shutil import copytree

    copytree(
        STAGES_DIR, os.path.join(repo.dvc_dir, "stages"), dirs_exist_ok=True
    )


def init(
    repo: "Repo",
    data: Dict[str, Optional[object]],
    template_name: str = None,
    interactive: bool = False,
    explicit: bool = False,
    template_loader: Callable[["Repo"], "BaseLoader"] = get_loader,
    force: bool = False,
):
    from jinja2 import DebugUndefined
    from jinja2.sandbox import ImmutableSandboxedEnvironment

    from dvc.dvcfile import make_dvcfile
    from dvc.utils.serialize import LOADERS, parse_yaml_for_update

    data = compact(data)  # remove None values
    loader = template_loader(repo)
    # let's start from strict for now
    environment = ImmutableSandboxedEnvironment(
        loader=loader, undefined=DebugUndefined
    )
    name = template_name or DEFAULT_TEMPLATE

    dvcfile = make_dvcfile(repo, "dvc.yaml")
    if not force and dvcfile.exists() and name in dvcfile.stages:
        raise DvcException(f"stage '{name}' already exists.")

    template = environment.get_template(f"{name}.yaml")
    context = ChainMap(data)

    local_context = context
    config: Dict[str, Any] = {}  # TODO
    defaults = DEFAULT_VALUES if not explicit else {}
    context = context.new_child(config)
    context = context.new_child(defaults)

    analyzer = Jinja2StaticAnalyzer(environment, template)
    if interactive:
        keys = analyzer.get_undefined_keys(
            local_context, ignore_variables=["param_names"]
        )
        defaults_context = context if not explicit else None
        response = compact(init_interactive(keys, defaults=defaults_context))
        context = context.new_child(response)

    param_path = context.get("params")
    if param_path:
        # See https://github.com/iterative/dvc/issues/6605 for the support
        # for depending on all params of a file.
        param_path = str(param_path)
        _, ext = os.path.splitext(str(param_path))
        params = list(LOADERS[ext](param_path))
        context = context.new_child({"param_names": params})

    # try to check for required variables that are missing
    undeclared_variables = analyzer.get_undefined_keys(context)
    if undeclared_variables:
        raise DvcException(
            f"template {template.name} has undefined variables: "
            f"{undeclared_variables}"
        )

    # render, parse yaml and build stage out of it
    rendered = template.render(**context)
    template_path = os.path.relpath(template.filename)
    data = parse_yaml_for_update(rendered, template_path)
    try:
        return repo.stage.add_from_dict(name, data, force=force)
    except MultipleInvalid as exc:
        raise DvcException(
            f"template '{template_path}'"
            "failed schema validation while rendering"
        ) from exc


if __name__ == "__main__":
    # pylint: disable=used-before-assignment
    from dvc.repo import Repo  # noqa: F811

    dvc_repo = Repo()
    init_template(dvc_repo)
