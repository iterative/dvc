import textwrap

import pytest

BASIC_CONFIG = textwrap.dedent(
    """\
        [feature]
            machine = true
        ['machine "foo"']
            cloud = aws
    """
)

TEST_INSTANCE = {
    "aws_security_group": None,
    "cloud": "aws",
    "id": "iterative-2jyhw8j9ieov6",
    "image": "ubuntu-bionic-18.04-amd64-server-20210818",
    "instance_gpu": None,
    "instance_hdd_size": 35,
    "instance_ip": "123.123.123.123",
    "instance_launch_time": "2021-08-25T07:13:03Z",
    "instance_type": "m",
    "name": "test-resource",
    "region": "us-west",
    "spot": False,
    "spot_price": -1,
    "ssh_name": None,
    "ssh_private": "-----BEGIN RSA PRIVATE KEY-----\\n",
    "startup_script": "IyEvYmluL2Jhc2g=",
    "timeouts": None,
}


@pytest.fixture
def machine_config(tmp_dir):
    (tmp_dir / ".dvc" / "config").write_text(BASIC_CONFIG)
    yield BASIC_CONFIG


@pytest.fixture
def machine_instance(tmp_dir, dvc, mocker):
    with dvc.config.edit() as conf:
        conf["machine"]["foo"] = {"cloud": "aws"}

    def mock_instances(name=None, **kwargs):
        if name == "foo":
            return iter([TEST_INSTANCE])
        return iter([])

    mocker.patch(
        "tpi.terraform.TerraformBackend.instances",
        mocker.MagicMock(side_effect=mock_instances),
    )
    yield TEST_INSTANCE
