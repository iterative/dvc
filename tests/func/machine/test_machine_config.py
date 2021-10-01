import os
import textwrap

import pytest
import tpi

from dvc.main import main

from . import CONFIG_TEXT


@pytest.mark.parametrize(
    "slot,value",
    [
        ("region", "us-west"),
        ("image", "iterative-cml"),
        ("name", "iterative_test"),
        ("spot", "True"),
        ("spot_price", "1.2345"),
        ("spot_price", "12345"),
        ("instance_hdd_size", "10"),
        ("instance_type", "l"),
        ("instance_gpu", "tesla"),
        ("ssh_private", "secret"),
    ],
)
def test_machine_modify_susccess(tmp_dir, dvc, slot, value):
    (tmp_dir / ".dvc" / "config").write_text(CONFIG_TEXT)
    assert main(["machine", "modify", "foo", slot, value]) == 0
    assert (
        tmp_dir / ".dvc" / "config"
    ).read_text() == CONFIG_TEXT + f"    {slot} = {value}\n"
    assert main(["machine", "modify", "--unset", "foo", slot]) == 0
    assert (tmp_dir / ".dvc" / "config").read_text() == CONFIG_TEXT


def test_machine_modify_startup_script(tmp_dir, dvc):
    slot, value = "startup_script", "start.sh"
    (tmp_dir / ".dvc" / "config").write_text(CONFIG_TEXT)
    assert main(["machine", "modify", "foo", slot, value]) == 0
    assert (
        tmp_dir / ".dvc" / "config"
    ).read_text() == CONFIG_TEXT + f"    {slot} = ../{value}\n"
    assert main(["machine", "modify", "--unset", "foo", slot]) == 0
    assert (tmp_dir / ".dvc" / "config").read_text() == CONFIG_TEXT


@pytest.mark.parametrize(
    "slot,value,msg",
    [
        (
            "region",
            "other-west",
            "expected one of us-west, us-east, eu-west, eu-north",
        ),
        ("spot_price", "NUM", "expected float"),
        ("instance_hdd_size", "BIG", "expected int"),
    ],
)
def test_machine_modify_fail(tmp_dir, dvc, caplog, slot, value, msg):
    (tmp_dir / ".dvc" / "config").write_text(CONFIG_TEXT)

    assert main(["machine", "modify", "foo", slot, value]) == 251
    assert (tmp_dir / ".dvc" / "config").read_text() == CONFIG_TEXT
    assert msg in caplog.text


FULL_CONFIG_TEXT = textwrap.dedent(
    """\
        [feature]
            machine = true
        ['machine \"bar\"']
            cloud = azure
        ['machine \"foo\"']
            cloud = aws
            region = us-west
            image = iterative-cml
            name = iterative_test
            spot = True
            spot_price = 1.2345
            instance_hdd_size = 10
            instance_type = l
            instance_gpu = tesla
            ssh_private = secret
            startup_script = {}
    """.format(
        os.path.join("..", "start.sh")
    )
)


def test_machine_list(tmp_dir, dvc, capsys):
    (tmp_dir / ".dvc" / "config").write_text(FULL_CONFIG_TEXT)

    assert main(["machine", "list"]) == 0
    cap = capsys.readouterr()
    assert "cloud=azure" in cap.out

    assert main(["machine", "list", "foo"]) == 0
    cap = capsys.readouterr()
    assert "cloud=azure" not in cap.out
    assert "cloud=aws" in cap.out
    assert "region=us-west" in cap.out
    assert "image=iterative-cml" in cap.out
    assert "name=iterative_test" in cap.out
    assert "spot=True" in cap.out
    assert "spot_price=1.2345" in cap.out
    assert "instance_hdd_size=10" in cap.out
    assert "instance_type=l" in cap.out
    assert "instance_gpu=tesla" in cap.out
    assert "ssh_private=secret" in cap.out
    assert (
        "startup_script={}".format(
            os.path.join(tmp_dir, ".dvc", "..", "start.sh")
        )
        in cap.out
    )


def test_machine_rename_success(tmp_dir, scm, dvc, capsys, mocker):
    config_file = tmp_dir / ".dvc" / "config"
    config_file.write_text(CONFIG_TEXT)

    mocker.patch.object(
        tpi.terraform.TerraformBackend,
        "state_mv",
        autospec=True,
        return_value=True,
    )

    os.makedirs((tmp_dir / ".dvc" / "tmp" / "machine" / "terraform" / "foo"))

    assert main(["machine", "rename", "foo", "bar"]) == 0
    cap = capsys.readouterr()
    assert "Rename machine 'foo' to 'bar'." in cap.out
    assert config_file.read_text() == CONFIG_TEXT.replace("foo", "bar")
    assert not (
        tmp_dir / ".dvc" / "tmp" / "machine" / "terraform" / "foo"
    ).exists()
    assert (
        tmp_dir / ".dvc" / "tmp" / "machine" / "terraform" / "bar"
    ).exists()


def test_machine_rename_none_exist(tmp_dir, scm, dvc, caplog):
    config_alice = CONFIG_TEXT.replace("foo", "alice")
    config_file = tmp_dir / ".dvc" / "config"
    config_file.write_text(config_alice)
    assert main(["machine", "rename", "foo", "bar"]) == 251
    assert config_file.read_text() == config_alice
    assert "machine 'foo' doesn't exist." in caplog.text


def test_machine_rename_exist(tmp_dir, scm, dvc, caplog):
    config_bar = CONFIG_TEXT + "['machine \"bar\"']\n    cloud = aws"
    config_file = tmp_dir / ".dvc" / "config"
    config_file.write_text(config_bar)
    assert main(["machine", "rename", "foo", "bar"]) == 251
    assert config_file.read_text() == config_bar
    assert "Machine 'bar' already exists." in caplog.text


def test_machine_rename_error(tmp_dir, scm, dvc, caplog, mocker):
    config_file = tmp_dir / ".dvc" / "config"
    config_file.write_text(CONFIG_TEXT)

    os.makedirs((tmp_dir / ".dvc" / "tmp" / "machine" / "terraform" / "foo"))

    def cmd_error(self, source, destination, **kwargs):
        raise tpi.TPIError("test error")

    mocker.patch.object(tpi.terraform.TerraformBackend, "state_mv", cmd_error)

    assert main(["machine", "rename", "foo", "bar"]) == 251
    assert config_file.read_text() == CONFIG_TEXT
    assert "rename failed" in caplog.text
