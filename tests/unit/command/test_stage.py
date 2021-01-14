import pytest

from dvc.cli import parse_args
from dvc.command.stage import CmdStageAdd


@pytest.mark.parametrize(
    "extra_args, expected_extra",
    [
        (["-c", "cmd1", "-c", "cmd2"], {"cmd": ["cmd1", "cmd2"]}),
        (["-c", "cmd1"], {"cmd": ["cmd1"]}),
    ],
)
def test_stage_add(mocker, dvc, extra_args, expected_extra):
    cli_args = parse_args(
        [
            "stage",
            "add",
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
            "--live-no-report",
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
            *extra_args,
        ]
    )
    assert cli_args.func == CmdStageAdd
    m = mocker.patch.object(CmdStageAdd, "create")

    cmd = cli_args.func(cli_args)

    assert cmd.run() == 0
    assert m.call_args[0] == (cmd.repo, True)

    expected = dict(
        name="name",
        deps=["deps"],
        outs=["outs"],
        outs_no_cache=["outs-no-cache"],
        params=["file:param1,param2", "param3"],
        metrics=["metrics"],
        metrics_no_cache=["metrics-no-cache"],
        plots=["plots"],
        plots_no_cache=["plots-no-cache"],
        live="live",
        live_no_summary=True,
        live_no_report=True,
        wdir="wdir",
        outs_persist=["outs-persist"],
        outs_persist_no_cache=["outs-persist-no-cache"],
        checkpoints=["checkpoints"],
        always_changed=True,
        external=True,
        desc="description",
        **expected_extra
    )
    # expected values should be a subset of what's in the call args list
    assert expected.items() <= m.call_args[1].items()
