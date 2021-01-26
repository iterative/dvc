import logging
from typing import TYPE_CHECKING

from voluptuous import MultipleInvalid

from dvc.exceptions import DvcException
from dvc.parsing.versions import SCHEMA_KWD

from .versions import ODB_VERSION

if TYPE_CHECKING:
    from dvc.tree.base import BaseTree

logger = logging.getLogger(__name__)


class ODBConfigFormatError(DvcException):
    pass


def get_odb_schema(d):
    from dvc.schema import COMPILED_ODB_CONFIG_V2_SCHEMA

    schema = {ODB_VERSION.V2: COMPILED_ODB_CONFIG_V2_SCHEMA}
    version = ODB_VERSION.from_dict(d)
    return schema[version]


class BaseODB:

    CONFIG_FILE = "dvc.odb.yaml"

    def __init__(self, tree: "BaseTree"):
        self.tree = tree
        self.config = self._load_config()
        self._migrate_config()

    def _load_config(self):
        from dvc.utils.serialize import load_yaml

        if self.tree.exists(self.CONFIG_FILE):
            data = load_yaml(self.CONFIG_FILE, tree=self.tree)
            try:
                self._validate_version(data)
                return data
            except MultipleInvalid:
                pass
        return {}

    @classmethod
    def _validate_version(cls, d):
        schema = get_odb_schema(d)
        try:
            return schema(d)
        except MultipleInvalid as exc:
            raise ODBConfigFormatError(
                f"'{cls.CONFIG_FILE}' format error: {exc}"
            )

    @property
    def version(self):
        return ODB_VERSION.from_dict(self.config)

    @property
    def latest_version_info(self):
        version = ODB_VERSION.V2.value  # pylint:disable=no-member
        return {SCHEMA_KWD: version}

    def _dump_config(self):
        from dvc.utils.serialize import modify_yaml

        logger.debug("Writing ODB config '%s'", self.CONFIG_FILE)
        with modify_yaml(self.CONFIG_FILE, tree=self.tree) as data:
            data.update(self.config)

    def _migrate_config(self):
        if self.version == ODB_VERSION.V1 and not self.tree.enable_dos2unix:
            logger.debug("Migrating ODB config '%s' to v2", self.CONFIG_FILE)
            self.config.update(self.latest_version_info)
            self._dump_config()
