import collections
import os

from dvc.cli import parse_args


def test_default(mocker, caplog):
    args = parse_args(["diff"])
    cmd = args.func(args)
    diff = {
        "added": [{"path": "file", "checksum": "00000000"}],
        "deleted": [],
        "modified": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert (
        "Added:\n"
        "    file\n"
        "\n"
        "summary: added (1), deleted (0), modified (0)"
    ) in caplog.text


def test_checksums(mocker, caplog):
    args = parse_args(["diff", "--checksums"])
    cmd = args.func(args)
    diff = {
        "added": [],
        "deleted": [
            {"path": os.path.join("data", ""), "checksum": "XXXXXXXX.dir"},
            {"path": os.path.join("data", "bar"), "checksum": "00000000"},
            {"path": os.path.join("data", "foo"), "checksum": "11111111"},
        ],
        "modified": [
            {
                "path": "file",
                "checksum": {"old": "AAAAAAAA", "new": "BBBBBBBB"},
            }
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
        "summary: added (0), deleted (2), modified (1)"
    ) in caplog.text


def test_json(mocker, caplog):
    args = parse_args(["diff", "--show-json"])
    cmd = args.func(args)
    diff = {
        "added": [{"path": "file", "checksum": "00000000"}],
        "deleted": [],
        "modified": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert '"added": [{"path": "file"}]' in caplog.text
    assert '"deleted": []' in caplog.text
    assert '"modified": []' in caplog.text


def test_json_checksums(mocker, caplog):
    args = parse_args(["diff", "--show-json", "--checksums"])
    cmd = args.func(args)

    diff = {
        "added": [
            # py35: maintain a consistent key order for tests purposes
            collections.OrderedDict(
                [("path", "file"), ("checksum", "00000000")]
            )
        ],
        "deleted": [],
        "modified": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert '"added": [{"path": "file", "checksum": "00000000"}]' in caplog.text
    assert '"deleted": []' in caplog.text
    assert '"modified": []' in caplog.text
