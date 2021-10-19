import json

from tpi.terraform import TerraformBackend

from dvc.command.machine import CmdMachineStatus
from dvc.main import main

from . import CONFIG_TEXT

STATUS = """{
    "aws_security_group": null,
    "cloud": "aws",
    "id": "iterative-2jyhw8j9ieov6",
    "image": "ubuntu-bionic-18.04-amd64-server-20210818",
    "instance_gpu": null,
    "instance_hdd_size": 35,
    "instance_ip": "123.123.123.123",
    "instance_launch_time": "2021-08-25T07:13:03Z",
    "instance_type": "m",
    "name": "test-resource",
    "region": "us-west",
    "spot": false,
    "spot_price": -1,
    "ssh_name": null,
    "ssh_private": "-----BEGIN RSA PRIVATE KEY-----\\n",
    "startup_script": "IyEvYmluL2Jhc2g=",
    "timeouts": null
}"""


def test_status(tmp_dir, scm, dvc, mocker, capsys):
    (tmp_dir / ".dvc" / "config").write_text(CONFIG_TEXT)
    status = json.loads(STATUS)

    mocker.patch.object(
        TerraformBackend, "instances", autospec=True, return_value=[status]
    )

    assert main(["machine", "status", "foo"]) == 0
    cap = capsys.readouterr()
    assert "instance_num_1:" in cap.out
    for key in CmdMachineStatus.SHOWN_FIELD:
        assert f"\t{key:20}: {status[key]}" in cap.out
