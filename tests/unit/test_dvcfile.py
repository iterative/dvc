import pytest

from dvc.dvcfile import (
    LOCK_FILE,
    PROJECT_FILE,
    FileIsGitIgnored,
    ProjectFile,
    SingleStageFile,
    load_file,
)
from dvc.stage import PipelineStage
from dvc.stage.exceptions import StageFileDoesNotExistError, StageFileIsNotDvcFileError
from dvc.utils.fs import remove
from dvc.utils.serialize import EncodingError
from dvc.utils.strictyaml import YAMLValidationError


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
    file_obj = load_file(object(), path)
    assert isinstance(file_obj, ProjectFile)


@pytest.mark.parametrize("path", ["Dvcfile", "stage.dvc", "../models/stage.dvc"])
def test_pipelines_single_stage_file(path):
    file_obj = load_file(object(), path)
    assert isinstance(file_obj, SingleStageFile)


@pytest.mark.parametrize("file", ["stage.dvc", "dvc.yaml"])
@pytest.mark.parametrize("is_dvcignored", [True, False])
def test_stage_load_on_not_existing_file(tmp_dir, dvc, file, is_dvcignored):
    dvcfile = load_file(dvc, file)
    if is_dvcignored:
        (tmp_dir / ".dvcignore").write_text(file)

    assert not dvcfile.exists()
    with pytest.raises(StageFileDoesNotExistError) as exc_info:
        assert dvcfile.stages.values()

    assert str(exc_info.value) == f"'{file}' does not exist"


@pytest.mark.parametrize("file", ["stage.dvc", "dvc.yaml"])
def test_stage_load_on_non_file(tmp_dir, dvc, file):
    (tmp_dir / file).mkdir()
    dvcfile = load_file(dvc, file)
    with pytest.raises(StageFileIsNotDvcFileError):
        assert dvcfile.stages.values()


@pytest.mark.parametrize("file", ["stage.dvc", "dvc.yaml"])
def test_stage_load_on_invalid_data(tmp_dir, dvc, file):
    data = {"is_this_a_valid_dvcfile": False}
    (tmp_dir / file).dump(data)
    dvcfile = load_file(dvc, file)
    with pytest.raises(YAMLValidationError):
        assert dvcfile.stages
    with pytest.raises(YAMLValidationError):
        assert dvcfile.validate(data, file)


def test_dump_stage(tmp_dir, dvc):
    stage = PipelineStage(dvc, cmd="command", name="stage_name", path="dvc.yaml")
    dvcfile = load_file(dvc, "dvc.yaml")

    dvcfile.dump(stage, update_lock=False, update_pipeline=False)
    assert not (tmp_dir / PROJECT_FILE).exists()
    assert not (tmp_dir / LOCK_FILE).exists()

    dvcfile.dump(stage, update_pipeline=False)
    assert not (tmp_dir / PROJECT_FILE).exists()
    assert (tmp_dir / LOCK_FILE).exists()
    assert dvcfile._lockfile.load()

    remove(tmp_dir / LOCK_FILE)

    dvcfile.dump(stage)
    assert (tmp_dir / PROJECT_FILE).exists()
    assert (tmp_dir / LOCK_FILE).exists()
    assert list(dvcfile.stages.values()) == [stage]


@pytest.mark.parametrize("file", ["stage.dvc", "dvc.yaml"])
def test_stage_load_file_exists_but_dvcignored(tmp_dir, dvc, scm, file):
    (tmp_dir / file).write_text("")
    (tmp_dir / ".dvcignore").write_text(file)

    dvc._reset()
    dvcfile = load_file(dvc, file)
    with pytest.raises(StageFileDoesNotExistError) as exc_info:
        assert dvcfile.stages.values()

    assert str(exc_info.value) == f"'{file}' is dvc-ignored"


@pytest.mark.parametrize("file", ["foo.dvc", "dvc.yaml"])
def test_try_loading_dvcfile_that_is_gitignored(tmp_dir, dvc, scm, file):
    with open(tmp_dir / ".gitignore", "a+", encoding="utf-8") as fd:
        fd.write(file)

    # create a file just to avoid other checks
    (tmp_dir / file).write_text("")
    scm._reset()

    dvcfile = load_file(dvc, file)
    with pytest.raises(FileIsGitIgnored) as exc_info:
        dvcfile._load()

    assert str(exc_info.value) == f"bad DVC file name '{file}' is git-ignored."


def test_dvcfile_encoding_error(tmp_dir, dvc):
    tmp_dir.gen(PROJECT_FILE, b"\x80some: stuff")

    dvcfile = load_file(dvc, PROJECT_FILE)
    with pytest.raises(EncodingError):
        dvcfile._load()
