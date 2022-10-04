from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, List

from hydra import compose, initialize_config_dir
from hydra._internal.config_loader_impl import ConfigLoaderImpl
from hydra._internal.core_plugins.basic_sweeper import BasicSweeper
from hydra.core.override_parser.overrides_parser import OverridesParser
from hydra.core.override_parser.types import ValueType
from hydra.errors import ConfigCompositionException, OverrideParseException
from omegaconf import OmegaConf

from dvc.exceptions import InvalidArgumentError

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
    parser = OverridesParser.create()
    return parser.parse_overrides(overrides=path_overrides)


def get_hydra_sweeps(path_overrides):
    merged_overrides = []
    for path, overrides in path_overrides.items():
        # `.` is reserved character in hydra syntax
        # _merge_ is required to support sweeps across multiple files.
        merged_overrides.extend(
            [
                f"{path.replace('.', '_')}_merge_{override}"
                for override in overrides
            ]
        )

    hydra_overrides = to_hydra_overrides(merged_overrides)
    for hydra_override in hydra_overrides:
        if hydra_override.value_type == ValueType.GLOB_CHOICE_SWEEP:
            raise InvalidArgumentError(
                f"Glob override '{hydra_override.input_line}' "
                "is not supported."
            )

    splits = BasicSweeper.split_arguments(hydra_overrides, None)[0]
    sweeps = []
    for split in splits:
        sweep_overrides = defaultdict(list)
        for merged_override in split:
            path, override = merged_override.split("_merge_")
            sweep_overrides[path.replace("_", ".")].append(override)
        sweeps.append(dict(sweep_overrides))
    return sweeps
