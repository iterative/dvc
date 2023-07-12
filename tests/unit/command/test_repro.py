from dvc.cli import parse_args
from dvc.commands.repro import CmdRepro

common_arguments = {
    "all_pipelines": False,
    "downstream": False,
    "dry": False,
    "force": False,
    "interactive": False,
    "pipeline": False,
    "single_item": False,
    "recursive": False,
    "force_downstream": False,
    "pull": False,
    "allow_missing": False,
    "targets": [],
    "on_error": "fail",
}
repro_arguments = {
    "run_cache": True,
    "no_commit": False,
    "glob": False,
}


def test_default_arguments(dvc, mocker):
    cmd = CmdRepro(parse_args(["repro"]))
    mocker.patch.object(cmd.repo, "reproduce")
    cmd.run()
    cmd.repo.reproduce.assert_called_with(**common_arguments, **repro_arguments)


def test_downstream(dvc, mocker):
    cmd = CmdRepro(parse_args(["repro", "--downstream"]))
    mocker.patch.object(cmd.repo, "reproduce")
    cmd.run()
    arguments = common_arguments.copy()
    arguments.update(repro_arguments)
    arguments.update({"downstream": True})
    cmd.repo.reproduce.assert_called_with(**arguments)
