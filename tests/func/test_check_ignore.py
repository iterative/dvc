import os

import pytest

from dvc.ignore import DvcIgnore
from dvc.main import main


@pytest.mark.parametrize(
    "file,ret,output", [("ignored", 0, "ignored\n"), ("not_ignored", 1, "")]
)
def test_check_ignore(tmp_dir, dvc, file, ret, output, caplog):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "ignored")

    assert main(["check-ignore", file]) == ret
    assert output in caplog.text


@pytest.mark.parametrize(
    "file,ret,output",
    [
        ("file", 0, "{}:1:f*\tfile\n".format(DvcIgnore.DVCIGNORE_FILE)),
        ("foo", 0, "{}:2:!foo\tfoo\n".format(DvcIgnore.DVCIGNORE_FILE)),
        (
            os.path.join("dir", "foobar"),
            0,
            "{}:1:foobar\t{}\n".format(
                os.path.join("dir", DvcIgnore.DVCIGNORE_FILE),
                os.path.join("dir", "foobar"),
            ),
        ),
    ],
)
def test_check_ignore_details(tmp_dir, dvc, file, ret, output, caplog):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "f*\n!foo")
    tmp_dir.gen({"dir": {DvcIgnore.DVCIGNORE_FILE: "foobar"}})

    assert main(["check-ignore", "-d", file]) == ret
    assert output in caplog.text


@pytest.mark.parametrize(
    "non_matching,output", [(["-n"], "::\tfile\n"), ([], "")]
)
def test_check_ignore_non_matching(tmp_dir, dvc, non_matching, output, caplog):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "other")

    assert main(["check-ignore", "-d"] + non_matching + ["file"]) == 1
    assert output in caplog.text


def test_check_ignore_non_matching_without_details(tmp_dir, dvc):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "other")

    assert main(["check-ignore", "-n", "file"]) == 255


def test_check_ignore_details_with_quiet(tmp_dir, dvc):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "other")

    assert main(["check-ignore", "-d", "-q", "file"]) == 255


@pytest.mark.parametrize("path,ret", [({"dir": {}}, 0), ({"dir": "files"}, 1)])
def test_check_ignore_dir(tmp_dir, dvc, path, ret):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/")
    tmp_dir.gen(path)

    assert main(["check-ignore", "-q", "dir"]) == ret


def test_check_ignore_default_dir(tmp_dir, dvc):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/")
    assert main(["check-ignore", "-q", ".dvc"]) == 1


def test_check_ignore_sub_repo(tmp_dir, dvc):
    tmp_dir.gen(
        {DvcIgnore.DVCIGNORE_FILE: "other", "dir": {".dvc": {}, "foo": "bar"}}
    )

    assert main(["check-ignore", "-q", os.path.join("dir", "foo")]) == 1


def test_check_sub_dir_ignore_file(tmp_dir, dvc, caplog):
    tmp_dir.gen(
        {
            DvcIgnore.DVCIGNORE_FILE: "other",
            "dir": {DvcIgnore.DVCIGNORE_FILE: "bar\nfoo", "foo": "bar"},
        }
    )

    assert main(["check-ignore", "-d", os.path.join("dir", "foo")]) == 0
    assert (
        "{}:2:foo\t{}".format(
            os.path.join("dir", DvcIgnore.DVCIGNORE_FILE),
            os.path.join("dir", "foo"),
        )
        in caplog.text
    )

    sub_dir = tmp_dir / "dir"
    with sub_dir.chdir():
        assert main(["check-ignore", "-d", "foo"]) == 0
        assert ".dvcignore:2:foo\tfoo" in caplog.text
