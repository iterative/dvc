from dvc.dvcfile import Dvcfile
from dvc.utils.serialize import dumps_yaml
from tests.unit.test_stage_resolver import (
    RESOLVED_DVC_YAML_DATA,
    TEMPLATED_DVC_YAML_DATA,
)


def test_parametrization_is_not_enabled_by_default(tmp_dir, dvc, mocker):
    assert dvc.config["feature"]["parametrization"] is False

    (tmp_dir / "dvc.yaml").write_text(dumps_yaml(RESOLVED_DVC_YAML_DATA))
    mock = mocker.patch("dvc.dvcfile.DataResolver")

    stages = list(Dvcfile(dvc, "dvc.yaml").stages)
    mock.assert_not_called()
    assert len(stages) == 2


def test_parametrization_flag_when_enabled(tmp_dir, dvc, mocker):
    dvc.config["feature"]["parametrization"] = True

    mock = mocker.patch(
        "dvc.dvcfile.DataResolver.resolve", return_value=RESOLVED_DVC_YAML_DATA
    )

    dvcfile = Dvcfile(dvc, "dvc.yaml")
    mocker.patch.object(
        dvcfile, "_load", return_value=[TEMPLATED_DVC_YAML_DATA, None]
    )

    stages = list(dvcfile.stages)
    mock.assert_called_once()
    assert len(stages) == 2
