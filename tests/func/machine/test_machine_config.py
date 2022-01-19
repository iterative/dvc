import os
import textwrap

import pytest
import tpi

from dvc.cli import main
from dvc.ui import ui
from tests.utils import console_width

from .conftest import BASIC_CONFIG


@pytest.mark.parametrize(
    "slot,value",
    [
        ("region", "us-west"),
        ("image", "iterative-cml"),
        ("spot", "True"),
        ("spot_price", "1.2345"),
        ("spot_price", "12345"),
        ("instance_hdd_size", "10"),
        ("instance_type", "l"),
        ("instance_gpu", "tesla"),
        ("ssh_private", "secret"),
    ],
)
def test_machine_modify_susccess(tmp_dir, dvc, machine_config, slot, value):
    assert main(["machine", "modify", "foo", slot, value]) == 0
    assert (
        tmp_dir / ".dvc" / "config"
    ).read_text() == machine_config + f"    {slot} = {value}\n"
    assert main(["machine", "modify", "--unset", "foo", slot]) == 0
    assert (tmp_dir / ".dvc" / "config").read_text() == machine_config


def test_machine_modify_startup_script(tmp_dir, dvc, machine_config):
    slot, value = "startup_script", "start.sh"
    assert main(["machine", "modify", "foo", slot, value]) == 0
    assert (
        tmp_dir / ".dvc" / "config"
    ).read_text() == machine_config + f"    {slot} = ../{value}\n"
    assert main(["machine", "modify", "--unset", "foo", slot]) == 0
    assert (tmp_dir / ".dvc" / "config").read_text() == machine_config


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
def test_machine_modify_fail(
    tmp_dir, dvc, machine_config, caplog, slot, value, msg
):
    assert main(["machine", "modify", "foo", slot, value]) == 251
    assert (tmp_dir / ".dvc" / "config").read_text() == machine_config
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
    from dvc.commands.machine import CmdMachineList

    (tmp_dir / ".dvc" / "config").write_text(FULL_CONFIG_TEXT)

    with console_width(ui.rich_console, 255):
        assert main(["machine", "list"]) == 0
    out, _ = capsys.readouterr()
    for key in CmdMachineList.TABLE_COLUMNS:
        assert f"{key}" in out
    assert "bar     azure    -         -" in out
    assert "foo     aws      us-west   iterative-cml  True    1.2345" in out
    assert "10                   l                ***            ***" in out
    assert "tesla" in out

    with console_width(ui.rich_console, 255):
        assert main(["machine", "list", "bar"]) == 0
    out, _ = capsys.readouterr()
    assert "foo" not in out
    assert "name    cloud" in out
    assert "bar     azure" in out


def test_machine_rename_success(
    tmp_dir, scm, dvc, machine_config, capsys, mocker
):
    config_file = tmp_dir / ".dvc" / "config"

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
    assert config_file.read_text() == machine_config.replace("foo", "bar")
    assert not (
        tmp_dir / ".dvc" / "tmp" / "machine" / "terraform" / "foo"
    ).exists()
    assert (
        tmp_dir / ".dvc" / "tmp" / "machine" / "terraform" / "bar"
    ).exists()


def test_machine_rename_none_exist(tmp_dir, scm, dvc, caplog):
    config_alice = BASIC_CONFIG.replace("foo", "alice")
    config_file = tmp_dir / ".dvc" / "config"
    config_file.write_text(config_alice)
    assert main(["machine", "rename", "foo", "bar"]) == 251
    assert config_file.read_text() == config_alice
    assert "machine 'foo' doesn't exist." in caplog.text


def test_machine_rename_exist(tmp_dir, scm, dvc, caplog):
    config_bar = BASIC_CONFIG + "['machine \"bar\"']\n    cloud = aws"
    config_file = tmp_dir / ".dvc" / "config"
    config_file.write_text(config_bar)
    assert main(["machine", "rename", "foo", "bar"]) == 251
    assert config_file.read_text() == config_bar
    assert "Machine 'bar' already exists." in caplog.text


def test_machine_rename_error(
    tmp_dir, scm, dvc, machine_config, caplog, mocker
):
    config_file = tmp_dir / ".dvc" / "config"
    os.makedirs((tmp_dir / ".dvc" / "tmp" / "machine" / "terraform" / "foo"))

    def cmd_error(self, source, destination, **kwargs):
        raise tpi.TPIError("test error")

    mocker.patch.object(tpi.terraform.TerraformBackend, "state_mv", cmd_error)

    assert main(["machine", "rename", "foo", "bar"]) == 251
    assert config_file.read_text() == machine_config
    assert "rename failed" in caplog.text
