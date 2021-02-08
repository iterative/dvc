import pytest

from dvc.cli import parse_args
from dvc.command.stage import CmdStageAdd


@pytest.mark.parametrize(
    "command, parsed_command",
    [
        (["echo", "foo", "bar"], "echo foo bar"),
        (["echo", '"foo bar"'], 'echo "foo bar"'),
        (["echo", "foo bar"], 'echo "foo bar"'),
    ],
)
def test_stage_add(mocker, dvc, command, parsed_command):
    cli_args = parse_args(
        [
            "stage",
            "add",
            "--name",
            "name",
            "--deps",
            "deps",
            "--outs",
            "outs",
            "--outs-no-cache",
            "outs-no-cache",
            "--metrics",
            "metrics",
            "--metrics-no-cache",
            "metrics-no-cache",
            "--plots",
            "plots",
            "--plots-no-cache",
            "plots-no-cache",
            "--live",
            "live",
            "--live-no-summary",
            "--live-no-html",
            "--wdir",
            "wdir",
            "--force",
            "--outs-persist",
            "outs-persist",
            "--outs-persist-no-cache",
            "outs-persist-no-cache",
            "--checkpoints",
            "checkpoints",
            "--always-changed",
            "--params",
            "file:param1,param2",
            "--params",
            "param3",
            "--external",
            "--desc",
            "description",
            "--force",
            *command,
        ]
    )
    assert cli_args.func == CmdStageAdd

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo.stage, "add")

    assert cmd.run() == 0
    expected = {
        "name": "name",
        "deps": ["deps"],
        "outs": ["outs"],
        "outs_no_cache": ["outs-no-cache"],
        "params": [
            {"file": ["param1", "param2"]},
            {"params.yaml": ["param3"]},
        ],
        "metrics": ["metrics"],
        "metrics_no_cache": ["metrics-no-cache"],
        "plots": ["plots"],
        "plots_no_cache": ["plots-no-cache"],
        "live": "live",
        "live_no_summary": True,
        "live_no_html": True,
        "wdir": "wdir",
        "outs_persist": ["outs-persist"],
        "outs_persist_no_cache": ["outs-persist-no-cache"],
        "checkpoints": ["checkpoints"],
        "always_changed": True,
        "external": True,
        "desc": "description",
        "cmd": parsed_command,
        "force": True,
    }
    args, kwargs = m.call_args
    assert not args

    for key, val in expected.items():
        # expected values should be a subset of what's in the call args list
        assert key in kwargs
        assert kwargs[key] == val, f"in key '{key}'"
