import os

import pytest

from dvc.ignore import DvcIgnore
from dvc.main import main


@pytest.mark.parametrize(
    "file,ret,output", [("ignored", 0, "ignored\n"), ("not_ignored", 1, "")]
)
def test_check_ignore(tmp_dir, dvc, file, ret, output, capsys):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "ignored")

    assert main(["check-ignore", file]) == ret
    captured = capsys.readouterr()
    run_output = captured.out
    assert run_output == output


@pytest.mark.parametrize(
    "file,ret,output",
    [
        ("file", 0, "{}:1:f*\tfile\n".format(DvcIgnore.DVCIGNORE_FILE)),
        ("foo", 1, "{}:2:!foo\tfoo\n".format(DvcIgnore.DVCIGNORE_FILE)),
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
def test_check_ignore_verbose(tmp_dir, dvc, file, ret, output, capsys):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "f*\n!foo")
    tmp_dir.gen({"dir": {DvcIgnore.DVCIGNORE_FILE: "foobar"}})

    assert main(["check-ignore", "-v", file]) == ret
    captured = capsys.readouterr()
    run_output = captured.out
    assert run_output == output


@pytest.mark.parametrize(
    "non_matching,output", [(["-n"], "::\tfile\n"), ([], "")]
)
def test_check_ignore_non_matching(tmp_dir, dvc, non_matching, output, capsys):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "other")

    assert main(["check-ignore", "-v"] + non_matching + ["file"]) == 1
    captured = capsys.readouterr()
    run_output = captured.out
    assert run_output == output


def test_check_ignore_non_matching_without_verbose(tmp_dir, dvc):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "other")

    assert main(["check-ignore", "-n", "file"]) == 255
