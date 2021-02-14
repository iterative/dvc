import logging
from typing import TYPE_CHECKING

from voluptuous import MultipleInvalid

from dvc.exceptions import DvcException

if TYPE_CHECKING:
    from dvc.fs.base import BaseFileSystem
    from dvc.path_info import PathInfo

logger = logging.getLogger(__name__)


CONFIG_FILENAME = "dvc.odb.yaml"


class ODBConfigFormatError(DvcException):
    pass


def load_config(path_info: "PathInfo", fs: "BaseFileSystem",) -> dict:
    from dvc.utils.serialize import load_yaml

    if fs.exists(path_info):
        data = load_yaml(path_info, fs=fs)
    else:
        data = {}

    try:
        _validate_version(data)
        return data
    except MultipleInvalid:
        pass
    return {}


def dump_config(
    config: dict, path_info: "PathInfo", fs: "BaseFileSystem",
):
    from dvc.utils.serialize import modify_yaml

    logger.debug("Writing ODB config '%s'", path_info)
    if not fs.exists(path_info.parent):
        fs.makedirs(path_info.parent)
    with modify_yaml(path_info, fs=fs) as data:
        data.update(config)


def migrate_config(config: dict) -> bool:
    from dvc.parsing.versions import SCHEMA_KWD

    from .versions import LATEST_VERSION

    if config.get(SCHEMA_KWD) != LATEST_VERSION:
        logger.debug("Migrating ODB config to '%s'", LATEST_VERSION)
        config[SCHEMA_KWD] = LATEST_VERSION
        return True
    return False


def _validate_version(d):
    schema = _get_config_schema(d)
    try:
        return schema(d)
    except MultipleInvalid as exc:
        raise ODBConfigFormatError("Could not read ODB config") from exc


def _get_config_schema(d):
    from dvc.schema import (
        COMPILED_ODB_CONFIG_V1_SCHEMA,
        COMPILED_ODB_CONFIG_V2_SCHEMA,
    )

    from .versions import ODB_VERSION

    schema = {
        ODB_VERSION.V1: COMPILED_ODB_CONFIG_V1_SCHEMA,
        ODB_VERSION.V2: COMPILED_ODB_CONFIG_V2_SCHEMA,
    }
    version = ODB_VERSION.from_dict(d)
    return schema[version]
