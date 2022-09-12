from pathlib import Path
from typing import TYPE_CHECKING, List

from hydra import compose, initialize_config_dir
from hydra._internal.config_loader_impl import ConfigLoaderImpl
from hydra.core.override_parser.overrides_parser import OverridesParser
from hydra.errors import ConfigCompositionException, OverrideParseException
from hydra.types import RunMode
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
            parser = OverridesParser.create()
            parsed = parser.parse_overrides(overrides=overrides)
            ConfigLoaderImpl.validate_sweep_overrides_legal(
                parsed, run_mode=RunMode.RUN, from_shell=True
            )

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
