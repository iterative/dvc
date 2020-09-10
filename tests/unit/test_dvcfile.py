import pytest

from dvc.dvcfile import (
    PIPELINE_FILE,
    PIPELINE_LOCK,
    Dvcfile,
    PipelineFile,
    SingleStageFile,
)
from dvc.stage import PipelineStage
from dvc.stage.exceptions import (
    StageFileDoesNotExistError,
    StageFileFormatError,
    StageFileIsNotDvcFileError,
)
from dvc.utils.fs import remove
from dvc.utils.serialize import dump_yaml


@pytest.mark.parametrize(
    "path",
    [
        "pipelines.yaml",
        "pipelines.yml",
        "custom-pipelines.yml",
        "custom-pipelines.yaml",
        "../models/pipelines.yml",
    ],
)
def test_pipelines_file(path):
    file_obj = Dvcfile(object(), path)
    assert isinstance(file_obj, PipelineFile)


@pytest.mark.parametrize(
    "path", ["Dvcfile", "stage.dvc", "../models/stage.dvc"]
)
def test_pipelines_single_stage_file(path):
    file_obj = Dvcfile(object(), path)
    assert isinstance(file_obj, SingleStageFile)


@pytest.mark.parametrize("file", ["stage.dvc", "dvc.yaml"])
def test_stage_load_on_not_existing_file(tmp_dir, dvc, file):
    dvcfile = Dvcfile(dvc, file)
    assert not dvcfile.exists()
    with pytest.raises(StageFileDoesNotExistError):
        assert dvcfile.stages.values()
    (tmp_dir / file).mkdir()
    with pytest.raises(StageFileIsNotDvcFileError):
        assert dvcfile.stages.values()


@pytest.mark.parametrize("file", ["stage.dvc", "dvc.yaml"])
def test_stage_load_on_invalid_data(tmp_dir, dvc, file):
    data = {"is_this_a_valid_dvcfile": False}
    dump_yaml(file, data)
    dvcfile = Dvcfile(dvc, file)
    with pytest.raises(StageFileFormatError):
        assert dvcfile.stages
    with pytest.raises(StageFileFormatError):
        assert dvcfile.validate(data, file)


def test_dump_stage(tmp_dir, dvc):
    stage = PipelineStage(
        dvc, cmd="command", name="stage_name", path="dvc.yaml"
    )
    dvcfile = Dvcfile(dvc, "dvc.yaml")

    dvcfile.dump(stage, update_lock=False, update_pipeline=False)
    assert not (tmp_dir / PIPELINE_FILE).exists()
    assert not (tmp_dir / PIPELINE_LOCK).exists()

    dvcfile.dump(stage, update_pipeline=False)
    assert not (tmp_dir / PIPELINE_FILE).exists()
    assert (tmp_dir / PIPELINE_LOCK).exists()
    assert dvcfile._lockfile.load()

    remove(tmp_dir / PIPELINE_LOCK)

    dvcfile.dump(stage)
    assert (tmp_dir / PIPELINE_FILE).exists()
    assert (tmp_dir / PIPELINE_LOCK).exists()
    assert list(dvcfile.stages.values()) == [stage]
