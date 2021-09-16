import os
import textwrap

import pytest

from dvc.main import main

CONFIG_TEXT = textwrap.dedent(
    """\
        [feature]
            machine = true
        ['machine \"foo\"']
            cloud = aws
    """
)


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
