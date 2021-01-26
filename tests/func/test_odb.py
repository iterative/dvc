import pytest

from dvc.odb.base import BaseODB


@pytest.mark.parametrize("dos2unix", [True, False])
def test_migrates_config_during_push(
    tmp_dir, dvc, local_remote, mocker, dos2unix
):
    dvc.config["core"]["dos2unix"] = dos2unix
    tmp_dir.dvc_gen("foo", "foo")

    migrate_spy = mocker.spy(BaseODB, "migrate_config")
    dump_spy = mocker.spy(BaseODB, "_dump_config")

    dvc.push()

    assert migrate_spy.mock.called
    assert dump_spy.mock.called != dos2unix


@pytest.mark.parametrize(
    "config", [{"schema": "1.1"}, {"schema": "2.1"}, {"schema": "3.0"}],
)
def test_config_invalid_versions(tmp_dir, dvc, config):
    from dvc.odb.base import ODBConfigFormatError

    with pytest.raises(ODBConfigFormatError):
        BaseODB._validate_version(config)
