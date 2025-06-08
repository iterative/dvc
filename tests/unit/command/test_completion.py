import logging

import pytest

from dvc.cli import main
from dvc.commands.completion import SUPPORTED_SHELLS


@pytest.mark.parametrize("shell", SUPPORTED_SHELLS)
def test_completion(caplog, capsys, shell):
    with caplog.at_level(logging.INFO):
        assert main(["completion", "-s", shell]) == 0
    assert not caplog.text

    out, err = capsys.readouterr()
    assert not err
    assert out
