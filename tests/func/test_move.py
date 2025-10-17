import os
import shutil
import textwrap

import pytest

from dvc.cli import main
from dvc.exceptions import MoveNotDataSourceError, OutputNotFoundError
from dvc.stage.exceptions import StageFileAlreadyExistsError


def test_move(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen("foo", "bar")
    assert (tmp_dir / "foo.dvc").exists()
    dvc.move("foo", "bar")

    assert not (tmp_dir / "foo.dvc").exists()
    assert (tmp_dir / "bar.dvc").exists()
    assert not (tmp_dir / "foo").is_file()
    assert (tmp_dir / "bar").is_file()
    # should only have the new path in the .gitignore, and only once
    assert (tmp_dir / ".gitignore").read_text().splitlines() == ["/bar"]


def test_move_non_existent_file(dvc):
    with pytest.raises(OutputNotFoundError):
        dvc.move("non_existent_file", "dst")


def test_move_missing_file(tmp_dir, dvc, scm, caplog):
    tmp_dir.dvc_gen("foo", "foo")
    (tmp_dir / "foo").unlink()
    contents = (tmp_dir / "foo.dvc").parse()
    dvc.move("foo", "bar")

    assert not (tmp_dir / "foo.dvc").exists()
    # only the path should be changed in the dvc file
    contents["outs"][0]["path"] = "bar"
    assert contents == (tmp_dir / "bar.dvc").parse()

    # file should not be checked out
    assert not (tmp_dir / "foo").is_file()
    assert not (tmp_dir / "bar").is_file()
    # should only have the new path in the .gitignore, and only once
    assert (tmp_dir / ".gitignore").read_text().splitlines() == ["/bar"]


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
    dvc.run(cmd="cp foo file1", outs=["file1"], deps=["foo"], name="copy-foo-file1")

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


def test_import(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen("foo", "foo", commit="add foo")
    imp_stage = dvc.imp(os.curdir, "foo", "foo_imported")

    dvc.move("foo_imported", "foo_moved")

    (stage,) = dvc.stage.collect("foo_moved.dvc")
    assert imp_stage.md5 != stage.md5
    res = (tmp_dir / "foo_moved.dvc").read_text()
    assert res == textwrap.dedent(
        f"""\
        md5: {stage.md5}
        frozen: true
        deps:
        - path: foo
          repo:
            url: {os.curdir}
            rev_lock: {scm.get_rev()}
        outs:
        - md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
          hash: md5
          path: foo_moved
    """
    )


@pytest.mark.parametrize(
    "path_func",
    [pytest.param(os.path.abspath, id="abs"), pytest.param(os.path.relpath, id="rel")],
)
def test_import_url_in_repo(tmp_dir, dvc, path_func):
    tmp_dir.gen("foo", "foo")
    imp_stage = dvc.imp_url(path_func(tmp_dir / "foo"), "foo_imported")
    (tmp_dir / "data").mkdir()

    dvc.move("foo_imported", os.path.join("data", "foo_moved"))

    (stage,) = dvc.stage.collect(os.path.join("data", "foo_moved.dvc"))
    assert imp_stage.md5 != stage.md5
    res = (tmp_dir / "data" / "foo_moved.dvc").read_text()
    assert res == textwrap.dedent(
        f"""\
        md5: {stage.md5}
        frozen: true
        deps:
        - md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
          hash: md5
          path: ../foo
        outs:
        - md5: acbd18db4cc2f85cedef654fccc4a4d8
          size: 3
          hash: md5
          path: foo_moved
    """
    )


@pytest.mark.parametrize(
    "path_func",
    [pytest.param(os.path.abspath, id="abs"), pytest.param(os.path.relpath, id="rel")],
)
def test_import_url_out_of_repo(tmp_dir, dvc, scm, path_func, make_tmp_dir):
    external = make_tmp_dir("external")
    external.gen("foo", "foo")

    imp_stage = dvc.imp_url(path_func(external / "foo"), "foo_imported")

    data_dir = tmp_dir / "data"
    data_dir.mkdir()

    new_path = data_dir / "foo_moved"
    new_dvcfile = new_path.with_suffix(".dvc")
    dvc.move("foo_imported", os.fspath(new_path))

    (stage,) = dvc.stage.collect(os.fspath(new_dvcfile))
    assert imp_stage.md5 != stage.md5

    with data_dir.chdir():
        expected_path = path_func(external / "foo")

    assert new_dvcfile.parse() == {
        "md5": stage.md5,
        "frozen": True,
        "deps": [
            {
                "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                "size": 3,
                "hash": "md5",
                "path": expected_path,
            }
        ],
        "outs": [
            {
                "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                "size": 3,
                "hash": "md5",
                "path": "foo_moved",
            }
        ],
    }


@pytest.mark.parametrize(
    "path_func",
    [pytest.param(os.path.abspath, id="abs"), pytest.param(os.path.relpath, id="rel")],
)
def test_all_metadata_are_preserved(tmp_dir, dvc, make_tmp_dir, path_func):
    external = make_tmp_dir("external")
    external.gen("foo", "foo")

    contents = {
        "md5": "bad",  # placeholder, does not matter for the test
        "frozen": True,
        "desc": "this is a stage description",
        "always_changed": True,
        "meta": {"custom_key": 42},
        "deps": [
            {
                "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                "size": 3,
                "hash": "md5",
                "path": path_func(external / "foo"),
            }
        ],
        "outs": [
            {
                "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                "path": "foo_imported",
                "persist": True,
                "hash": "md5",
                "size": 3,
                "desc": "this is a description",
                "type": "model",
                "labels": ["label1", "label2"],
                "meta": {"custom_key": 42},
                "cache": False,
                "remote": "myremote",
                "push": False,
            }
        ],
    }
    (tmp_dir / "foo_imported.dvc").dump(contents)
    (tmp_dir / "foo_imported").write_text("foo")

    data_dir = tmp_dir / "data"
    data_dir.mkdir()

    new_path = data_dir / "foo_moved"
    new_dvcfile = new_path.with_suffix(".dvc")
    dvc.move("foo_imported", os.fspath(new_path))

    (stage,) = dvc.stage.collect(os.fspath(new_dvcfile))

    with data_dir.chdir():
        expected_path = path_func(external / "foo")

    contents["outs"][0] |= {"path": "foo_moved"}
    contents["deps"][0] |= {"path": expected_path}
    contents |= {"md5": stage.md5}
    assert new_dvcfile.parse() == contents


def test_move_dst_stage_file_already_exists(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo", "bar": "bar"})

    with pytest.raises(StageFileAlreadyExistsError) as exc_info:
        dvc.move("foo", "bar")
    assert str(exc_info.value) == "'bar.dvc' already exists"
    assert exc_info.value.__cause__ is None
