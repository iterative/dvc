import collections
import logging
import os

from dvc.cli import parse_args
from dvc.command.diff import _show_md


def test_default(mocker, caplog):
    args = parse_args(["diff"])
    cmd = args.func(args)
    diff = {
        "added": [{"path": "file", "hash": "00000000"}],
        "deleted": [],
        "modified": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert (
        "Added:\n"
        "    file\n"
        "\n"
        "files summary: 1 added, 0 deleted, 0 modified"
    ) in caplog.text


def test_show_hash(mocker, caplog):
    args = parse_args(["diff", "--show-hash"])
    cmd = args.func(args)
    diff = {
        "added": [],
        "deleted": [
            {"path": os.path.join("data", ""), "hash": "XXXXXXXX.dir"},
            {"path": os.path.join("data", "bar"), "hash": "00000000"},
            {"path": os.path.join("data", "foo"), "hash": "11111111"},
        ],
        "modified": [
            {"path": "file", "hash": {"old": "AAAAAAAA", "new": "BBBBBBBB"}}
        ],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)
    assert 0 == cmd.run()
    assert (
        "Deleted:\n"
        "    XXXXXXXX  " + os.path.join("data", "") + "\n"
        "    00000000  " + os.path.join("data", "bar") + "\n"
        "    11111111  " + os.path.join("data", "foo") + "\n"
        "\n"
        "Modified:\n"
        "    AAAAAAAA..BBBBBBBB  file\n"
        "\n"
        "files summary: 0 added, 2 deleted, 1 modified"
    ) in caplog.text


def test_show_json(mocker, caplog):
    args = parse_args(["diff", "--show-json"])
    cmd = args.func(args)
    diff = {
        "added": [{"path": "file", "hash": "00000000"}],
        "deleted": [],
        "modified": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert '"added": [{"path": "file"}]' in caplog.text
    assert '"deleted": []' in caplog.text
    assert '"modified": []' in caplog.text


def test_show_json_and_hash(mocker, caplog):
    args = parse_args(["diff", "--show-json", "--show-hash"])
    cmd = args.func(args)

    diff = {
        "added": [
            # py35: maintain a consistent key order for tests purposes
            collections.OrderedDict([("path", "file"), ("hash", "00000000")])
        ],
        "deleted": [],
        "modified": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert '"added": [{"path": "file", "hash": "00000000"}]' in caplog.text
    assert '"deleted": []' in caplog.text
    assert '"modified": []' in caplog.text


def test_no_changes(mocker, caplog):
    args = parse_args(["diff", "--show-json"])
    cmd = args.func(args)
    mocker.patch("dvc.repo.Repo.diff", return_value={})

    def info():
        return [
            msg
            for name, level, msg in caplog.record_tuples
            if name.startswith("dvc") and level == logging.INFO
        ]

    assert 0 == cmd.run()
    assert ["{}"] == info()

    caplog.clear()

    args = parse_args(["diff"])
    cmd = args.func(args)
    assert 0 == cmd.run()
    assert not info()


def test_show_md_empty():
    assert _show_md({}) == ("| Status   | Path   |\n|----------|--------|\n")


def test_show_md():
    diff = {
        "deleted": [
            {"path": "zoo", "hash": "22222"},
            {"path": os.path.join("data", ""), "hash": "XXXXXXXX.dir"},
            {"path": os.path.join("data", "foo"), "hash": "11111111"},
            {"path": os.path.join("data", "bar"), "hash": "00000000"},
        ],
        "modified": [
            {"path": "file", "hash": {"old": "AAAAAAAA", "new": "BBBBBBBB"}}
        ],
        "added": [{"path": "file", "hash": "00000000"}],
    }
    assert _show_md(diff) == (
        "| Status   | Path     |\n"
        "|----------|----------|\n"
        "| added    | file     |\n"
        "| deleted  | data{sep}    |\n"
        "| deleted  | data{sep}bar |\n"
        "| deleted  | data{sep}foo |\n"
        "| deleted  | zoo      |\n"
        "| modified | file     |\n"
    ).format(sep=os.path.sep)
