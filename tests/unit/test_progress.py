import logging
from dvc.progress import Tqdm
import sys


def test_quiet(caplog, capsys):
    with caplog.at_level(logging.CRITICAL, logger="dvc"):
        for _ in Tqdm(range(10)):
            pass
        out_err = capsys.readouterr()
        assert out_err.out == ""
        assert out_err.err == ""
    with caplog.at_level(logging.INFO, logger="dvc"):
        for _ in Tqdm(range(10)):
            pass
        out_err = capsys.readouterr()
        assert out_err.out == ""
        assert out_err.err == ""

        # simulate interactive terminal
        sys.stderr.isatty = lambda: True
        for _ in Tqdm(range(10)):
            pass
        out_err = capsys.readouterr()
        assert out_err.out == ""
        assert "0/10" in out_err.err
