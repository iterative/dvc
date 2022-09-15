import os
from pathlib import Path

import pytest

from dvc.cli import main
from dvc.ignore import DvcIgnore


def _get_tmp_config_dir(tmp_path, level):
    return tmp_path / ".." / (tmp_path.name + "_config_" + level)

@pytest.fixture(autouse=True)
def tmp_config_dir(mocker, tmp_path):
    """
    Fixture to prevent modifying/reading the actual global config
    """

    for level in ["global", "system"]:
        os.makedirs(_get_tmp_config_dir(tmp_path, level))

    def get_tmp_config_dir(level):
        return str(_get_tmp_config_dir(tmp_path, level))
    mocker.patch("dvc.config.Config.get_dir", side_effect=get_tmp_config_dir)

@pytest.mark.parametrize(
    "file,ret,output", [("ignored", 0, True), ("not_ignored", 1, False)]
)
def test_check_ignore(tmp_dir, dvc, file, ret, output, caplog, capsys):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "ignored")

    assert main(["check-ignore", file]) == ret

    out, _ = capsys.readouterr()
    assert (file in out) is output
    assert "Having any troubles?" not in caplog.text


@pytest.mark.parametrize(
    "file,ret,output",
    [
        ("file", 0, f"{DvcIgnore.DVCIGNORE_FILE}:1:f*\tfile\n"),
        ("foo", 0, f"{DvcIgnore.DVCIGNORE_FILE}:2:!foo\tfoo\n"),
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
def test_check_ignore_details(tmp_dir, dvc, file, ret, output, capsys):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "f*\n!foo")
    tmp_dir.gen({"dir": {DvcIgnore.DVCIGNORE_FILE: "foobar"}})

    assert main(["check-ignore", "-d", file]) == ret
    assert (output, "") == capsys.readouterr()


@pytest.mark.parametrize("non_matching", [True, False])
def test_check_ignore_non_matching(tmp_dir, dvc, non_matching, caplog, capsys):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "other")
    if non_matching:
        assert main(["check-ignore", "-d", "-n", "file"]) == 1
    else:
        assert main(["check-ignore", "-d", "file"]) == 1

    out, _ = capsys.readouterr()
    assert ("::\tfile\n" in out) is non_matching


@pytest.mark.parametrize(
    "args",
    [
        ["-n", "file"],
        ["-a", "file"],
        ["-q", "-d", "file"],
        ["--stdin", "file"],
        [],
    ],
)
def test_check_ignore_error_args_cases(tmp_dir, dvc, args, caplog):
    assert main(["check-ignore"] + args) == 255
    assert "Having any troubles?" not in caplog.text


@pytest.mark.parametrize("path,ret", [({"dir": {}}, 0), ({"dir": "files"}, 1)])
def test_check_ignore_dir(tmp_dir, dvc, path, ret):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "dir/")
    tmp_dir.gen(path)

    assert main(["check-ignore", "-q", "dir"]) == ret


def test_check_ignore_default_dir(tmp_dir, dvc):
    assert main(["check-ignore", "-q", ".dvc"]) == 1


def test_check_ignore_out_side_repo(tmp_dir, dvc):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "file")
    assert main(["check-ignore", "-q", "../file"]) == 1


def test_check_ignore_sub_repo(tmp_dir, dvc, capsys):
    tmp_dir.gen(
        {DvcIgnore.DVCIGNORE_FILE: "other", "dir": {".dvc": {}, "foo": "bar"}}
    )

    assert main(["check-ignore", "-d", os.path.join("dir", "foo")]) == 0
    out, _ = capsys.readouterr()
    assert (
        "in sub_repo:{}\t{}".format("dir", os.path.join("dir", "foo")) in out
    )


def test_check_excludesfile(tmp_dir, dvc, capsys):
    excludesfile = Path.home() / DvcIgnore.DVCIGNORE_FILE

    with dvc.config.edit() as conf:
        conf["core"]["excludesfile"] = str(excludesfile)
    tmp_dir.gen(
        {
            "dir": {
                "ignored_in_repo_root": "ignored_in_repo_root",
                "ignored_in_excludesfile": "ignored_in_excludesfile",
            }
        }
    )
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "ignored_in_repo_root")
    (excludesfile).write_text(
        "ignored_in_excludesfile", encoding="utf-8"
    )

    assert main(["check-ignore", "-d", "ignored_in_repo_root"]) == 0
    output, _ = capsys.readouterr()
    assert (
        output
        == f"{DvcIgnore.DVCIGNORE_FILE}:1:ignored_in_repo_root\t"
        + "ignored_in_repo_root\n"
    )

    assert main(["check-ignore", "-d", "ignored_in_excludesfile"]) == 0
    output, _ = capsys.readouterr()
    assert output == f"{excludesfile}:1:ignored_in_excludesfile\tignored_in_excludesfile\n"

def test_check_global_dvcignore(tmp_path, tmp_dir, dvc, capsys):
    tmp_dir.gen(
        {
            "dir": {
                "ignored_in_repo_root": "ignored_in_repo_root",
                "ignored_in_global": "ignored_in_global",
            }
        }
    )
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "ignored_in_repo_root")
    global_dvcignore = _get_tmp_config_dir(tmp_path, "global") / DvcIgnore.DVCIGNORE_FILE
    global_dvcignore.write_text(
        "ignored_in_global",
        encoding="utf-8"
    )

    assert main(["check-ignore", "-d", "ignored_in_repo_root"]) == 0
    output, _ = capsys.readouterr()
    assert (
        output
        == f"{DvcIgnore.DVCIGNORE_FILE}:1:ignored_in_repo_root\t"
        + "ignored_in_repo_root\n"
    )

    assert main(["check-ignore", "-d", "ignored_in_global"]) == 0
    output, _ = capsys.readouterr()
    assert output == f"{global_dvcignore}:1:ignored_in_global\tignored_in_global\n"

def test_check_system_dvcignore(tmp_path, tmp_dir, dvc, capsys):
    tmp_dir.gen(
        {
            "dir": {
                "ignored_in_repo_root": "ignored_in_repo_root",
                "ignored_in_system": "ignored_in_system",
            }
        }
    )
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "ignored_in_repo_root")
    system_dvcignore = _get_tmp_config_dir(tmp_path, "system") / DvcIgnore.DVCIGNORE_FILE
    system_dvcignore.write_text(
        "ignored_in_system",
        encoding="utf-8"
    )

    assert main(["check-ignore", "-d", "ignored_in_repo_root"]) == 0
    output, _ = capsys.readouterr()
    assert (
        output
        == f"{DvcIgnore.DVCIGNORE_FILE}:1:ignored_in_repo_root\t"
        + "ignored_in_repo_root\n"
    )

    assert main(["check-ignore", "-d", "ignored_in_system"]) == 0
    output, _ = capsys.readouterr()
    assert output == f"{system_dvcignore}:1:ignored_in_system\tignored_in_system\n"

def test_check_sub_dir_ignore_file(tmp_dir, dvc, capsys):
    tmp_dir.gen(
        {
            DvcIgnore.DVCIGNORE_FILE: "other",
            "dir": {DvcIgnore.DVCIGNORE_FILE: "bar\nfoo", "foo": "bar"},
        }
    )

    assert main(["check-ignore", "-d", os.path.join("dir", "foo")]) == 0

    out, _ = capsys.readouterr()
    assert (
        "{}:2:foo\t{}".format(
            os.path.join("dir", DvcIgnore.DVCIGNORE_FILE),
            os.path.join("dir", "foo"),
        )
        in out
    )

    sub_dir = tmp_dir / "dir"
    with sub_dir.chdir():
        assert main(["check-ignore", "-d", "foo"]) == 0
        out, _ = capsys.readouterr()
        assert ".dvcignore:2:foo\tfoo" in out


def test_check_ignore_details_all(tmp_dir, dvc, capsys):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "f*\n!foo")

    assert main(["check-ignore", "-d", "-a", "foo"]) == 0
    out, _ = capsys.readouterr()
    assert f"{DvcIgnore.DVCIGNORE_FILE}:1:f*\tfoo\n" in out
    assert f"{DvcIgnore.DVCIGNORE_FILE}:2:!foo\tfoo\n" in out


@pytest.mark.parametrize(
    "file,ret,output", [("ignored", 0, True), ("not_ignored", 1, False)]
)
def test_check_ignore_stdin_mode(
    tmp_dir, dvc, file, ret, output, capsys, mocker
):
    tmp_dir.gen(DvcIgnore.DVCIGNORE_FILE, "ignored")
    mocker.patch("builtins.input", side_effect=[file, ""])

    assert main(["check-ignore", "--stdin"]) == ret
    out, _ = capsys.readouterr()
    assert (file in out) is output
