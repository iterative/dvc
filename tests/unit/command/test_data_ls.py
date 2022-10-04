import pytest

from dvc.cli import parse_args
from dvc.commands.data import CmdDataLs


def test_cli(mocker):
    mocker.patch("dvc.repo.Repo")
    ls = mocker.patch("dvc.repo.data.ls", return_value={})
    show_table_spy = mocker.spy(CmdDataLs, "_show_table")

    cli_args = parse_args(
        [
            "data",
            "ls",
            "--labels",
            "label1,label2",
            "--labels",
            "label3",
            "--type",
            "type1,type2",
            "--type",
            "type3",
            "target",
            "--recursive",
            "--md",
        ]
    )

    assert cli_args.func == CmdDataLs
    cmd = cli_args.func(cli_args)
    assert cmd.run() == 0

    ls.assert_called_once_with(
        cmd.repo,
        targets=["target"],
        recursive=True,
    )
    show_table_spy.assert_called_once_with(
        {},
        filter_labels={"label1", "label2", "label3"},
        filter_types={"type1", "type2", "type3"},
        markdown=True,
    )


EXAMPLE_DATA = [
    {
        "path": "model.pkl",
        "desc": "desc",
        "type": "model",
        "labels": ["get-started", "dataset-registry"],
        "meta": {"key": "value", "key1": "value1"},
    },
    {
        "path": "model.pkl",
        "desc": "desc",
        "type": "model",
        "labels": ["get-started", "example"],
    },
    {"path": "unlabeled.txt"},
]


@pytest.mark.parametrize(
    "markdown, expected_output",
    [
        (
            True,
            """\
| Path      | Type   | Labels                       | Description   |
|-----------|--------|------------------------------|---------------|
| model.pkl | model  | get-started,dataset-registry | desc          |
| model.pkl | model  | get-started,example          | desc          |\n""",
        ),
        (
            False,
            """\
 Path       Type   Labels                        Description
 model.pkl  model  get-started,dataset-registry  desc
 model.pkl  model  get-started,example           desc""",
        ),
    ],
    ids=["markdown", "default"],
)
def test_ls(capsys, markdown, expected_output):
    CmdDataLs._show_table(
        EXAMPLE_DATA,
        filter_types={"model"},
        filter_labels={"get-started", "dataset-registry"},
        markdown=markdown,
    )
    out, err = capsys.readouterr()
    assert not err
    out = "\n".join(line.rstrip() for line in out.splitlines())
    assert out == expected_output
