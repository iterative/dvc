import pytest


def test_validate_name():
    from dvc.exceptions import InvalidArgumentError
    from dvc.machine import RESERVED_NAMES, validate_name

    for name in RESERVED_NAMES:
        with pytest.raises(InvalidArgumentError):
            validate_name(name)


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_get_config_and_backend(tmp_dir, dvc, cloud):
    from dvc.machine.backend.terraform import TerraformBackend

    name = "foo"
    with dvc.config.edit() as conf:
        conf["machine"][name] = {"cloud": cloud}
    config, backend = dvc.machine.get_config_and_backend(name)
    assert config == {"cloud": cloud, "name": name}
    assert isinstance(backend, TerraformBackend)


def test_get_config_and_backend_nonexistent(tmp_dir, dvc):
    from dvc.config import MachineNotFoundError

    with pytest.raises(MachineNotFoundError):
        dvc.machine.get_config_and_backend("foo")


def test_get_config_and_backend_default(tmp_dir, dvc):
    from dvc.config import NoMachineError

    with pytest.raises(NoMachineError):
        dvc.machine.get_config_and_backend()
