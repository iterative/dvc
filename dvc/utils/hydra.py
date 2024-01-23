from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from dvc.exceptions import InvalidArgumentError
from dvc.log import logger

from .collections import merge_dicts, remove_missing_keys, to_omegaconf

if TYPE_CHECKING:
    from dvc.types import StrPath


logger = logger.getChild(__name__)


def load_hydra_plugins(plugins_path: str):
    import sys

    from hydra.core.plugins import Plugins

    sys.path.append(plugins_path)
    try:
        Plugins.instance()
    finally:
        sys.path.remove(plugins_path)


def compose_and_dump(
    output_file: "StrPath",
    config_dir: Optional[str],
    config_module: Optional[str],
    config_name: str,
    plugins_path: str,
    overrides: List[str],
) -> None:
    """Compose Hydra config and dumpt it to `output_file`.

    Args:
        output_file: File where the composed config will be dumped.
        config_dir: Folder containing the Hydra config files.
            Must be absolute file system path.
        config_module: Module containing the Hydra config files.
            Ignored if `config_dir` is not `None`.
        config_name: Name of the config file containing defaults,
            without the .yaml extension.
        plugins_path: Path to auto discover Hydra plugins.
        overrides: List of `Hydra Override`_ patterns.

    .. _Hydra Override:
        https://hydra.cc/docs/advanced/override_grammar/basic/
    """
    from hydra import compose, initialize_config_dir, initialize_config_module
    from omegaconf import OmegaConf

    from .serialize import DUMPERS

    config_source = config_dir or config_module
    if not config_source:
        raise ValueError("Either `config_dir` or `config_module` should be provided.")
    initialize_config = (
        initialize_config_dir if config_dir else initialize_config_module
    )

    load_hydra_plugins(plugins_path)
    with initialize_config(  # type: ignore[attr-defined]
        config_source, version_base=None
    ):
        cfg = compose(config_name=config_name, overrides=overrides)

    OmegaConf.resolve(cfg)

    suffix = Path(output_file).suffix.lower()
    if suffix not in [".yml", ".yaml"]:
        dumper = DUMPERS[suffix]
        dumper(output_file, OmegaConf.to_object(cfg))
    else:
        Path(output_file).write_text(OmegaConf.to_yaml(cfg), encoding="utf-8")
    logger.trace(
        "Hydra composition enabled. Contents dumped to %s:\n %s", output_file, cfg
    )


def apply_overrides(path: "StrPath", overrides: List[str]) -> None:
    """Update `path` params with the provided `Hydra Override`_ patterns.

    Args:
        overrides: List of `Hydra Override`_ patterns.

    .. _Hydra Override:
        https://hydra.cc/docs/next/advanced/override_grammar/basic/
    """
    from hydra._internal.config_loader_impl import ConfigLoaderImpl
    from hydra.errors import ConfigCompositionException, OverrideParseException
    from omegaconf import OmegaConf

    from .serialize import MODIFIERS

    suffix = Path(path).suffix.lower()

    hydra_errors = (ConfigCompositionException, OverrideParseException)

    modify_data = MODIFIERS[suffix]
    with modify_data(path) as original_data:
        try:
            parsed = to_hydra_overrides(overrides)

            new_data = OmegaConf.create(
                to_omegaconf(original_data),
                flags={"allow_objects": True},
            )
            OmegaConf.set_struct(new_data, True)

            ConfigLoaderImpl._apply_overrides_to_config(parsed, new_data)
            new_data = OmegaConf.to_object(new_data)
        except hydra_errors as e:
            raise InvalidArgumentError("Invalid `--set-param` value") from e

        merge_dicts(original_data, new_data)
        remove_missing_keys(original_data, new_data)


def to_hydra_overrides(path_overrides):
    from hydra.core.override_parser.overrides_parser import OverridesParser

    parser = OverridesParser.create()
    return parser.parse_overrides(overrides=path_overrides)


def dict_product(dicts):
    import itertools

    return [dict(zip(dicts, x)) for x in itertools.product(*dicts.values())]


def get_hydra_sweeps(path_overrides):
    from hydra._internal.core_plugins.basic_sweeper import BasicSweeper
    from hydra.core.override_parser.types import ValueType

    path_sweeps = {}
    for path, overrides in path_overrides.items():
        overrides = to_hydra_overrides(overrides)
        for override in overrides:
            if override.value_type == ValueType.GLOB_CHOICE_SWEEP:
                raise InvalidArgumentError(
                    f"Glob override '{override.input_line}' is not supported."
                )
        path_sweeps[path] = BasicSweeper.split_arguments(overrides, None)[0]
    return dict_product(path_sweeps)
