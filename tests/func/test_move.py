import os
import shutil
import textwrap

import pytest

from dvc.cli import main
from dvc.exceptions import MoveNotDataSourceError, OutputNotFoundError


def test_move(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    dvc.move("foo", "foo1")

    assert not (tmp_dir / "foo").is_file()
    assert (tmp_dir / "foo1").is_file()


def test_move_non_existent_file(dvc):
    with pytest.raises(OutputNotFoundError):
        dvc.move("non_existent_file", "dst")


def test_move_directory(tmp_dir, dvc):
    tmp_dir.dvc_gen("data", {"foo": "foo", "bar": "bar"})
    dvc.move("data", "dst")
    assert not (tmp_dir / "data").is_dir()
    assert (tmp_dir / "dst").is_dir()


def test_cmd_move(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    assert main(["move", "foo", "foo1"]) == 0
    assert main(["move", "non-existing-file", "dst"]) != 0


def test_move_not_data_source(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    dvc.run(
        cmd="cp foo file1",
        outs=["file1"],
        deps=["foo"],
        name="copy-foo-file1",
    )

    with pytest.raises(MoveNotDataSourceError):
        dvc.move("file1", "dst")

    assert main(["move", "file1", "dst"]) != 0
    assert (tmp_dir / "file1").exists()


def test_move_file_with_extension(tmp_dir, dvc):
    tmp_dir.dvc_gen("file.csv", "1,2,3\n")

    assert main(["move", "file.csv", "other_name.csv"]) == 0
    assert not (tmp_dir / "file.csv").exists()
    assert not (tmp_dir / "file.csv.dvc").exists()
    assert (tmp_dir / "other_name.csv").exists()
    assert (tmp_dir / "other_name.csv.dvc").exists()


def test_move_file_to_directory(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    tmp_dir.gen({"data": {"bar": "bar"}})

    assert main(["move", "foo", os.path.join("data", "foo")]) == 0
    assert not (tmp_dir / "foo").exists()
    assert not (tmp_dir / "foo.dvc").exists()
    assert (tmp_dir / "data" / "foo").exists()
    assert (tmp_dir / "data" / "foo.dvc").exists()


def test_move_file_to_directory_without_specified_target_name(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    tmp_dir.gen({"data": {"bar": "bar"}})

    assert main(["move", "foo", "data"]) == 0
    assert not (tmp_dir / "foo").exists()
    assert not (tmp_dir / "foo.dvc").exists()
    assert (tmp_dir / "data" / "foo").exists()
    assert (tmp_dir / "data" / "foo.dvc").exists()

    new_stage = (tmp_dir / "data" / "foo.dvc").load_yaml()
    assert new_stage["outs"][0]["path"] == "foo"


def test_move_directory_should_not_overwrite_existing(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"data": {"foo": "foo"}})
    new_dir = tmp_dir / "dir"
    new_dir.mkdir()

    dvc.move("data", "dir")
    assert not (tmp_dir / "data").exists()
    assert not (tmp_dir / "data.dvc").exists()
    assert set(new_dir.iterdir()) == {
        new_dir / ".gitignore",
        new_dir / "data.dvc",
        new_dir / "data",
    }
    assert set((new_dir / "data").iterdir()) == {new_dir / "data" / "foo"}


def test_move_file_between_directories(tmp_dir, dvc):
    tmp_dir.gen({"data": {"foo": "foo"}})
    dvc.add(os.path.join("data", "foo"))

    (tmp_dir / "data2").mkdir()

    assert main(["move", os.path.join("data", "foo"), "data2"]) == 0
    assert not (tmp_dir / "data" / "foo").exists()
    assert not (tmp_dir / "data" / "foo.dvc").exists()
    assert (tmp_dir / "data2" / "foo").exists()
    assert (tmp_dir / "data2" / "foo.dvc").exists()

    d = (tmp_dir / "data2" / "foo.dvc").load_yaml()
    assert d["outs"][0]["path"] == "foo"


def test_move_file_inside_directory(tmp_dir, dvc):
    tmp_dir.gen({"data": {"foo": "foo"}})
    file = tmp_dir / "data" / "foo"
    dvc.add(file.fs_path)

    with (tmp_dir / "data").chdir():
        assert main(["move", "foo", "data.txt"]) == 0

    assert not file.exists()
    assert (tmp_dir / "data" / "data.txt").exists()
    assert (tmp_dir / "data" / "data.txt.dvc").exists()


def test_move_should_save_stage_info(tmp_dir, dvc):
    tmp_dir.dvc_gen({"old_name": {"file1": "file1"}})

    dvc.move("old_name", "new_name")

    assert dvc.status() == {}


def test_should_move_to_dir_on_non_default_stage_file(tmp_dir, dvc):
    tmp_dir.gen({"file": "file_content"})

    dvc.add("file")
    shutil.move("file.dvc", "stage.dvc")
    os.mkdir("directory")

    dvc.move("file", "directory")

    assert os.path.exists(os.path.join("directory", "file"))


def test_move_gitignored(tmp_dir, scm, dvc):
    from dvc.dvcfile import FileIsGitIgnored

    tmp_dir.dvc_gen({"foo": "foo"})

    os.mkdir("dir")
    (tmp_dir / "dir").gen(".gitignore", "*")

    with pytest.raises(FileIsGitIgnored):
        dvc.move("foo", "dir")

    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "foo.dvc").exists()
    assert not (tmp_dir / "dir" / "foo").exists()
    assert not (tmp_dir / "dir" / "foo.dvc").exists()


def test_move_output_overlap(tmp_dir, dvc):
    from dvc.exceptions import OverlappingOutputPathsError

    tmp_dir.dvc_gen({"foo": "foo", "dir": {"bar": "bar"}})

    with pytest.raises(OverlappingOutputPathsError):
        dvc.move("foo", "dir")

    assert (tmp_dir / "foo").read_text() == "foo"
    assert (tmp_dir / "foo.dvc").exists()
    assert not (tmp_dir / "dir" / "foo").exists()
    assert not (tmp_dir / "dir" / "foo.dvc").exists()


def test_move_meta(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    data = (tmp_dir / stage.path).parse()
    data["meta"] = {"custom_key": 42}
    (tmp_dir / stage.path).dump(data)

    dvc.move("foo", "bar")
    res = (tmp_dir / "bar.dvc").read_text()
    assert res == textwrap.dedent(
        """\
        outs:
        - md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
          hash: md5
          path: bar
        meta:
          custom_key: 42
    """
    )
