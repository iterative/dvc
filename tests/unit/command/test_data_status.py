import json

import pytest
from funcy import omit

from dvc.cli import main, parse_args
from dvc.commands.data import CmdDataStatus
from dvc.repo import Repo
from dvc.repo.data import Status
from tests.func.parsing.test_errors import escape_ansi


@pytest.fixture
def mocked_status():
    return Status(
        not_in_cache=["notincache"],
        committed={
            "added": ["dir/bar", "dir/foo"],
            "deleted": ["dir/baz"],
            "modified": ["dir/foobar"],
            "unknown": ["dir/unknown1"],
        },
        uncommitted={
            "added": ["dir/baz"],
            "modified": ["dir/bar"],
            "deleted": ["dir/foobar"],
            "unknown": ["dir2/unknown2"],
        },
        untracked=["untracked"],
        unchanged=["dir/foo"],
        git={"is_dirty": True, "is_empty": False},
    )


def test_cli(dvc, mocker, mocked_status):
    status = mocker.patch("dvc.repo.Repo.data_status", return_value=mocked_status)

    cli_args = parse_args(
        [
            "data",
            "status",
            "--json",
            "--unchanged",
            "--untracked-files",
            "--granular",
        ]
    )

    assert cli_args.func == CmdDataStatus
    cmd = cli_args.func(cli_args)
    assert cmd.run() == 0
    status.assert_called_once_with(
        targets=[],
        untracked_files="all",
        not_in_remote=False,
        remote_refresh=True,
        granular=True,
    )


@pytest.mark.parametrize(
    "args, to_omit",
    [
        ([], ["untracked", "unchanged"]),
        (["--unchanged"], ["untracked"]),
        (["--unchanged", "--untracked-files"], []),
    ],
)
def test_json(dvc, mocker, capsys, mocked_status, args, to_omit):
    mocker.patch("dvc.repo.Repo.data_status", return_value=mocked_status)
    assert main(["data", "status", "--json", *args]) == 0
    out, err = capsys.readouterr()
    assert out.rstrip() == json.dumps(omit(mocked_status, [*to_omit, "git"]))
    assert not err


def test_no_changes_repo(dvc, scm, capsys):
    assert main(["data", "status"]) == 0
    out, _ = capsys.readouterr()
    assert out == "No changes.\n"


def test_empty_scm_repo(tmp_dir, capsys):
    tmp_dir.init(scm=True)
    Repo.init()

    assert main(["data", "status"]) == 0
    out, _ = capsys.readouterr()
    assert (
        out
        == """\
No changes in an empty git repo.
(there are changes not tracked by dvc, use "git status" to see)
"""
    )


@pytest.mark.parametrize(
    "args",
    [
        ("--untracked-files",),
        ("--unchanged",),
        ("--untracked-files", "--unchanged"),
    ],
)
@pytest.mark.parametrize("is_dirty", [True, False])
def test_show_status(dvc, scm, mocker, capsys, mocked_status, args, is_dirty):
    mocked_status["git"]["is_dirty"] = is_dirty
    mocker.patch("dvc.repo.Repo.data_status", return_value=mocked_status)
    assert main(["data", "status", *args]) == 0
    out, err = capsys.readouterr()
    expected_out = """\
Not in cache:
  (use "dvc fetch <file>..." to download files)
        notincache

DVC committed changes:
  (git commit the corresponding dvc files to update the repo)
        added: dir/bar
        added: dir/foo
        deleted: dir/baz
        modified: dir/foobar
        unknown: dir/unknown1

DVC uncommitted changes:
  (use "dvc commit <file>..." to track changes)
  (use "dvc checkout <file>..." to discard changes)
        added: dir/baz
        modified: dir/bar
        deleted: dir/foobar
        unknown: dir2/unknown2
"""
    if "--untracked-files" in args:
        expected_out += """
Untracked files:
  (use "git add <file> ..." or "dvc add <file>..." to commit to git or to dvc)
        untracked
"""
    if "--unchanged" in args:
        expected_out += """
DVC unchanged files:
        dir/foo
"""

    if is_dirty:
        expected_out += """\
(there are other changes not tracked by dvc, use "git status" to see)
"""
    assert escape_ansi(out) == expected_out
    assert not err
