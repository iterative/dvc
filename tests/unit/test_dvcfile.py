import pytest

from dvc.dvcfile import Dvcfile, PipelineFile, SingleStageFile


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
