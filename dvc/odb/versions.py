import enum
from collections.abc import Mapping

from voluptuous import validators

from dvc.parsing.versions import SCHEMA_KWD


def odb_version_schema(value):
    expected = [ODB_VERSION.V2.value]  # pylint: disable=no-member
    msg = "invalid schema version {}, expected one of {}".format(
        value, expected
    )
    return validators.Any(*expected, msg=msg)(value)


class VersionEnum(str, enum.Enum):
    @classmethod
    def all_versions(cls):
        return [v.value for v in cls]


class ODB_VERSION(VersionEnum):
    V1 = "1.0"  # DVC <2.0 (dos2unix MD5)
    V2 = "2.0"  # DVC 2.x (standard MD5)

    @classmethod
    def from_dict(cls, data):
        # 1) if it's empty or or is not a dict, use the oldest one (V1).
        # 2) use the `schema` identifier if it exists and is a supported
        # version
        # 3) if it's not in any of the supported version, use the latest one
        # 4) if there's no identifier, it's a V1
        if not data or not isinstance(data, Mapping):
            return cls(cls.V1)

        version = data.get(SCHEMA_KWD)
        if version:
            return cls(version if version in cls.all_versions() else cls.V2)
        return cls(cls.V1)
