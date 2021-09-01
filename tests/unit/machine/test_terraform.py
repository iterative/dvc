import json
import os

import pytest
from mock import ANY, MagicMock
from python_terraform import IsFlagged

from dvc.machine.backend.terraform import DvcTerraform, TerraformError

TEST_RESOURCE_STATE = """{
  "version": 4,
  "terraform_version": "1.0.4",
  "serial": 12,
  "lineage": "7f93cd8b-3abf-b4a4-d939-60b55699c3ed",
  "outputs": {},
  "resources": [
    {
      "mode": "managed",
      "type": "iterative_machine",
      "name": "test-resource",
      "provider": "provider[\\"registry.terraform.io/iterative/iterative\\"]",
      "instances": [
        {
          "schema_version": 0,
          "attributes": {
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
          },
          "sensitive_attributes": [],
          "private": ""
        }
      ]
    }
  ]
}
"""


@pytest.fixture
def terraform(tmp_dir, mocker):
    from python_terraform import Terraform

    from dvc.machine.backend.terraform import TerraformBackend

    cmd_mock = mocker.patch.object(
        Terraform, "cmd", return_value=(0, None, None)
    )
    tf = TerraformBackend(tmp_dir)
    tf.cmd_mock = cmd_mock
    yield tf


@pytest.fixture
def resource(tmp_dir):
    name = "test-resource"
    (tmp_dir / name).gen("terraform.tfstate", TEST_RESOURCE_STATE)
    yield name


def test_run_cmd(mocker):
    from python_terraform import Terraform

    tf = DvcTerraform()
    cmd_mock = mocker.patch.object(
        Terraform, "cmd", return_value=(0, None, None)
    )
    tf.cmd("init")
    cmd_mock.assert_called_once_with("init", capture_output=False)

    cmd_mock = mocker.patch.object(
        Terraform, "cmd", return_value=(-1, None, None)
    )
    with pytest.raises(TerraformError):
        tf.cmd("init")


def test_make_pemfile():
    content = "foo\nbar\n"
    with DvcTerraform.pemfile({"ssh_private": content}) as path:
        with open(path, "r") as fobj:
            assert fobj.read() == content


def test_make_tf(terraform):
    name = "foo"
    with terraform.make_tf(name) as tf:
        assert tf.working_dir == os.path.join(terraform.tmp_dir, name)


def test_create(tmp_dir, terraform):
    name = "foo"
    terraform.create(name=name, cloud="aws")
    terraform.cmd_mock.assert_any_call("init", capture_output=False)
    terraform.cmd_mock.assert_any_call(
        "apply", capture_output=False, auto_approve=IsFlagged
    )
    assert (tmp_dir / name / "main.tf.json").exists()
    with open(tmp_dir / name / "main.tf.json") as fobj:
        data = json.load(fobj)
    assert data["resource"]["iterative_machine"] == {
        name: {"name": name, "cloud": "aws"},
    }


def test_destroy(tmp_dir, terraform, resource):
    terraform.destroy(name=resource, cloud="aws")
    terraform.cmd_mock.assert_called_with(
        "destroy", capture_output=False, auto_approve=IsFlagged
    )


def test_instances(tmp_dir, terraform, resource):
    data = json.loads(TEST_RESOURCE_STATE)
    expected = data["resources"][0]["instances"][0]["attributes"]
    assert list(terraform.instances(resource)) == [expected]


def test_shell(tmp_dir, terraform, resource, mocker):
    data = json.loads(TEST_RESOURCE_STATE)
    expected = data["resources"][0]["instances"][0]["attributes"]
    mock_connect = mocker.patch("asyncssh.connect", return_value=MagicMock())
    terraform.run_shell(name=resource)
    mock_connect.assert_called_once_with(
        host=expected["instance_ip"],
        username="ubuntu",
        client_keys=ANY,
        known_hosts=None,
    )
