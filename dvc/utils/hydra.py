import sys
from pathlib import Path
from typing import TYPE_CHECKING, List

try:
    from hydra import compose, initialize_config_dir
    from hydra._internal.config_loader_impl import ConfigLoaderImpl
    from hydra._internal.core_plugins.basic_sweeper import BasicSweeper
    from hydra.core.override_parser.overrides_parser import OverridesParser
    from hydra.core.override_parser.types import ValueType
    from hydra.errors import ConfigCompositionException, OverrideParseException
    from omegaconf import OmegaConf

    hydra_compatible = True
except ValueError:
    if sys.version_info >= (3, 11):
        hydra_compatible = False
    else:
        raise

from dvc.exceptions import DvcException, InvalidArgumentError

from .collections import merge_dicts, remove_missing_keys, to_omegaconf
from .serialize import DUMPERS, MODIFIERS

if TYPE_CHECKING:
    from dvc.types import StrPath


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
    if not hydra_compatible:
        raise DvcException(
            "hydra functionality is not supported in Python >= 3.11"
        )

    with initialize_config_dir(config_dir, version_base=None):
        cfg = compose(config_name=config_name, overrides=overrides)

    dumper = DUMPERS[Path(output_file).suffix.lower()]
    dumper(output_file, OmegaConf.to_object(cfg))


def apply_overrides(path: "StrPath", overrides: List[str]) -> None:
    """Update `path` params with the provided `Hydra Override`_ patterns.

    Args:
        overrides: List of `Hydra Override`_ patterns.

    .. _Hydra Override:
        https://hydra.cc/docs/next/advanced/override_grammar/basic/
    """
    if not hydra_compatible:
        raise DvcException(
            "hydra functionality is not supported in Python >= 3.11"
        )

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
    if not hydra_compatible:
        raise DvcException(
            "hydra functionality is not supported in Python >= 3.11"
        )

    parser = OverridesParser.create()
    return parser.parse_overrides(overrides=path_overrides)


def dict_product(dicts):
    import itertools

    return [dict(zip(dicts, x)) for x in itertools.product(*dicts.values())]


def get_hydra_sweeps(path_overrides):
    if not hydra_compatible:
        raise DvcException(
            "hydra functionality is not supported in Python >= 3.11"
        )

    path_sweeps = {}
    for path, overrides in path_overrides.items():
        overrides = to_hydra_overrides(overrides)
        for override in overrides:
            if override.value_type == ValueType.GLOB_CHOICE_SWEEP:
                raise InvalidArgumentError(
                    f"Glob override '{override.input_line}' "
                    "is not supported."
                )
        path_sweeps[path] = BasicSweeper.split_arguments(overrides, None)[0]
    return dict_product(path_sweeps)
