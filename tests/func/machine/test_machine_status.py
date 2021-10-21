from dvc.command.machine import CmdMachineStatus
from dvc.main import main


def test_status(
    tmp_dir, scm, dvc, machine_config, machine_instance, mocker, capsys
):
    status = machine_instance
    assert main(["machine", "status", "foo"]) == 0
    cap = capsys.readouterr()
    assert "instance_num_1:" in cap.out
    for key in CmdMachineStatus.SHOWN_FIELD:
        assert f"\t{key:20}: {status[key]}" in cap.out
