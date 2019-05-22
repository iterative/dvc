from dvc.command.repro import CmdRepro
from dvc.cli import parse_args

default_arguments = {
    "all_pipelines": False,
    "downstream": False,
    "dry": False,
    "force": False,
    "ignore_build_cache": False,
    "interactive": False,
    "no_commit": False,
    "pipeline": False,
    "recursive": True,
}


def test_default_arguments(dvc, mocker):
    cmd = CmdRepro(parse_args(["repro"]))
    mocker.patch.object(cmd.repo, "reproduce")
    cmd.run()
    cmd.repo.reproduce.assert_called_with("Dvcfile", **default_arguments)


def test_downstream(dvc, mocker):
    cmd = CmdRepro(parse_args(["repro", "--downstream"]))
    mocker.patch.object(cmd.repo, "reproduce")
    cmd.run()
    arguments = default_arguments.copy()
    arguments.update({"downstream": True})
    cmd.repo.reproduce.assert_called_with("Dvcfile", **arguments)
