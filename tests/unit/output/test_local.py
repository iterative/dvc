import os

from dvc.output import Output
from dvc.stage import Stage
from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.meta import Meta


def test_str_workdir_outside_repo(tmp_dir, erepo_dir):
    stage = Stage(erepo_dir.dvc)
    output = Output(stage, "path", cache=False)

    assert os.path.abspath("path") == str(output)


def test_str_workdir_inside_repo(dvc):
    stage = Stage(dvc)
    output = Output(stage, "path", cache=False)

    assert str(output) == "path"

    stage = Stage(dvc, wdir="some_folder")
    output = Output(stage, "path", cache=False)

    assert os.path.join("some_folder", "path") == str(output)


def test_str_on_local_absolute_path(dvc):
    stage = Stage(dvc)

    rel_path = os.path.join("path", "to", "file")
    abs_path = os.path.abspath(rel_path)
    output = Output(stage, abs_path, cache=False)

    assert output.def_path == rel_path
    assert output.fs_path == abs_path
    assert str(output) == rel_path


def test_str_on_external_absolute_path(dvc):
    stage = Stage(dvc)

    rel_path = os.path.join("..", "path", "to", "file")
    abs_path = os.path.abspath(rel_path)
    output = Output(stage, abs_path, cache=False)

    assert output.def_path == abs_path
    assert output.fs_path == abs_path
    assert str(output) == abs_path


def test_return_0_on_no_cache(dvc):
    o = Output(Stage(dvc), "path")
    o.use_cache = False
    assert o.get_files_number() == 0


def test_return_multiple_for_dir(dvc):
    o = Output(Stage(dvc), "path")
    o.hash_info = HashInfo("md5", "12345678.dir")
    o.meta = Meta(nfiles=2)
    assert o.get_files_number() == 2


def test_return_1_on_single_file_cache(mocker, dvc):
    mocker.patch.object(Output, "is_dir_checksum", False)
    o = Output(Stage(dvc), "path")
    o.hash_info = HashInfo("md5", "12345678")
    assert o.get_files_number() == 1
