import logging
from pathlib import Path
from typing import TYPE_CHECKING, List

from dvc.exceptions import InvalidArgumentError

from .collections import merge_dicts, remove_missing_keys, to_omegaconf

if TYPE_CHECKING:
    from dvc.types import StrPath


logger = logging.getLogger(__name__)


def compose_and_dump(
    output_file: "StrPath",
    config_dir: str,
    config_name: str,
    overrides: List[str],
) -> None:
    """Compose Hydra config and dumpt it to `output_file`.

    Args:
        output_file: File where the composed config will be dumped.
        config_dir: Folder containing the Hydra config files.
            Must be absolute file system path.
        config_name: Name of the config file containing defaults,
            without the .yaml extension.
        overrides: List of `Hydra Override`_ patterns.

    .. _Hydra Override:
        https://hydra.cc/docs/advanced/override_grammar/basic/
    """
    from hydra import compose, initialize_config_dir
    from omegaconf import OmegaConf

    from .serialize import DUMPERS

    with initialize_config_dir(config_dir, version_base=None):
        cfg = compose(config_name=config_name, overrides=overrides)

    OmegaConf.resolve(cfg)

    suffix = Path(output_file).suffix.lower()
    if suffix not in [".yml", ".yaml"]:
        dumper = DUMPERS[suffix]
        dumper(output_file, OmegaConf.to_object(cfg))
    else:
        Path(output_file).write_text(OmegaConf.to_yaml(cfg), encoding="utf-8")
    logger.trace(  # type: ignore[attr-defined]
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
            # pylint: disable=protected-access
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
