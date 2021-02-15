import os

import pytest

from dvc.odb.config import CONFIG_FILENAME
from dvc.odb.versions import ODB_VERSION


@pytest.mark.parametrize(
    "dos2unix, version", [(True, ODB_VERSION.V1), (False, ODB_VERSION.V2)],
)
def test_config_created(tmp_dir, dvc, local_remote, mocker, dos2unix, version):
    dvc.config["core"]["dos2unix"] = dos2unix
    tmp_dir.dvc_gen("foo", "foo")

    dvc.push()

    assert (local_remote / CONFIG_FILENAME).read_text().strip() == (
        f"schema: '{version.value}'"
    )


def test_config_incompatible_md5(tmp_dir, dvc):
    from dvc.exceptions import DvcException

    tmp_dir.gen(
        os.path.join(".dvc", "cache", CONFIG_FILENAME), "schema: '2.0'"
    )
    with pytest.raises(DvcException):
        dvc.config["core"]["dos2unix"] = True
        tmp_dir.dvc_gen("foo", "foo")


def test_config_migration(tmp_dir, dvc, local_remote):
    tmp_dir.gen(
        os.path.join(".dvc", "cache", CONFIG_FILENAME), "schema: '1.0'"
    )
    local_remote.gen(CONFIG_FILENAME, "schema: '1.0'")
    tmp_dir.dvc_gen("foo", "foo")
    dvc.push()

    assert (
        tmp_dir / ".dvc" / "cache" / CONFIG_FILENAME
    ).read_text().strip() == "schema: '2.0'"
    assert (
        local_remote / CONFIG_FILENAME
    ).read_text().strip() == "schema: '2.0'"


@pytest.mark.parametrize(
    "config", [{"schema": "1.1"}, {"schema": "2.1"}, {"schema": "3.0"}],
)
def test_config_invalid_versions(tmp_dir, dvc, config):
    from dvc.odb.config import ODBConfigFormatError, _validate_version

    with pytest.raises(ODBConfigFormatError):
        _validate_version(config)
