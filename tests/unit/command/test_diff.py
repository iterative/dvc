import json

from dvc.cli import parse_args


def test_default(mocker, caplog):
    args = parse_args(["diff"])
    cmd = args.func(args)
    diff = {
        "added": [{"filename": "file", "checksum": "00000000"}],
        "deleted": [],
        "modified": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert "Added:\n    file\n" in caplog.text


def test_checksums(mocker, caplog):
    args = parse_args(["diff", "--checksums"])
    cmd = args.func(args)
    diff = {
        "added": [],
        "deleted": [
            {"filename": "bar", "checksum": "00000000"},
            {"filename": "foo", "checksum": "11111111"},
        ],
        "modified": [
            {
                "filename": "file",
                "checksum": {"old": "AAAAAAAA", "new": "BBBBBBBB"},
            }
        ],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)
    assert 0 == cmd.run()
    assert (
        "Deleted:\n"
        "    00000000  bar\n"
        "    11111111  foo\n"
        "\n"
        "Modified:\n"
        "    AAAAAAAA..BBBBBBBB  file\n"
    ) in caplog.text


def test_json(mocker, caplog):
    args = parse_args(["diff", "--json"])
    cmd = args.func(args)
    diff = {
        "added": [{"filename": "file", "checksum": "00000000"}],
        "deleted": [],
        "modified": [],
    }
    mocker.patch("dvc.repo.Repo.diff", return_value=diff)

    assert 0 == cmd.run()
    assert json.dumps(diff) in caplog.text
