from dvc.dvcfile import Dvcfile
from dvc.stage.loader import StageLoader
from dvc.utils.serialize import dumps_yaml
from tests.unit.test_stage_resolver import (
    RESOLVED_DVC_YAML_DATA,
    TEMPLATED_DVC_YAML_DATA,
)


def test_parametrization_is_not_enabled_by_default(tmp_dir, dvc, mocker):
    assert dvc.config["feature"]["parametrization"] is False

    (tmp_dir / "dvc.yaml").write_text(dumps_yaml(RESOLVED_DVC_YAML_DATA))
    mock = mocker.patch("dvc.stage.loader.DataResolver")

    stages = list(Dvcfile(dvc, "dvc.yaml").stages)
    mock.assert_not_called()
    assert len(stages) == 2


def test_parametrization_flag_when_enabled(tmp_dir, dvc, mocker):
    dvc.config["feature"]["parametrization"] = True

    dvcfile = Dvcfile(dvc, "dvc.yaml")
    loader = StageLoader(dvcfile, TEMPLATED_DVC_YAML_DATA, lockfile_data=None)
    loader.resolved_data = RESOLVED_DVC_YAML_DATA["stages"]

    stages = list(loader.values())
    assert len(stages) == 2
