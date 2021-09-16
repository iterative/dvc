import dataclasses
import os
from collections import ChainMap
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, Optional

from funcy import compact
from voluptuous import MultipleInvalid, Schema

from dvc.exceptions import DvcException
from dvc.schema import STAGE_DEFINITION

if TYPE_CHECKING:
    from jinja2 import BaseLoader

    from dvc.repo import Repo


DEFAULT_TEMPLATE = "default"


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
STAGE_SCHEMA = Schema(STAGE_DEFINITION)


def get_loader(repo: "Repo") -> "BaseLoader":
    from jinja2 import ChoiceLoader, FileSystemLoader

    default_path = Path(__file__).parents[3] / "resources" / "stages"
    return ChoiceLoader(
        [
            # not initialized yet
            FileSystemLoader(Path(repo.dvc_dir) / "stages"),
            # won't work for other packages
            FileSystemLoader(default_path),
        ]
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
    from jinja2 import Environment

    from dvc.utils import relpath
    from dvc.stage import check_circular_dependency, check_duplicated_arguments
    from dvc.stage.loader import StageLoader
    from dvc.utils.serialize import LOADERS, parse_yaml_for_update

    data = compact(data)  # remove None values
    loader = template_loader(repo)
    environment = Environment(loader=loader)
    name = template_name or DEFAULT_TEMPLATE

    dvcfile = make_dvcfile(repo, "dvc.yaml")
    if not force and dvcfile.exists() and name in dvcfile.stages:
        raise DvcException(f"stage '{name}' already exists.")

    template = environment.get_template(f"{name}.yaml")
    context = ChainMap(data)
    if interactive:
        # TODO: interactive requires us to check for variables present
        #  in the template and, adapt our prompts accordingly.
        raise NotImplementedError("'-i/--interactive' is not supported yet.")
    if not explicit:
        context.maps.append(DEFAULT_VALUES)
    else:
        # TODO: explicit requires us to check for undefined variables.
        raise NotImplementedError("'--explicit' is not implemented yet.")

    assert "params" in context
    # See https://github.com/iterative/dvc/issues/6605 for the support
    # for depending on all params of a file.
    param_path = str(context["params"])
    _, ext = os.path.splitext(param_path)
    param_names = list(LOADERS[ext](param_path))

    # render, parse yaml and then validate schema
    rendered = template.render(**context, param_names=param_names)
    template_path = relpath(template.filename)
    data = parse_yaml_for_update(rendered, template_path)
    try:
        validated = STAGE_SCHEMA(data)
    except MultipleInvalid as exc:
        raise DvcException(
            f"template '{template_path}' "
            "failed schema validation while rendering"
        ) from exc

    stage = StageLoader.load_stage(dvcfile, name, validated)
    # ensure correctness, similar to what we have in `repo.stage.add`
    check_circular_dependency(stage)
    check_duplicated_arguments(stage)
    new_index = repo.index.add(stage)
    new_index.check_graph()

    with repo.scm.track_file_changes(config=repo.config):
        # note that we are not dumping the "template" as-is
        # we are dumping a stage data, which is processed
        # so formatting-wise, it may look different.
        stage.dump(update_lock=False)
        stage.ignore_outs()

    return stage
