import logging
from dvc.progress import Tqdm, TQDM_DISABLE
import sys


def test_quiet_logging(caplog, capsys):
    with caplog.at_level(logging.CRITICAL, logger="dvc"):
        for _ in Tqdm(range(10)):
            pass
        out_err = capsys.readouterr()
        assert out_err.out == ""
        if TQDM_DISABLE is False:  # False but not None
            assert "0/10" in out_err.err
        else:
            assert out_err.err == ""


def test_quiet_notty(caplog, capsys):
    with caplog.at_level(logging.INFO, logger="dvc"):
        for _ in Tqdm(range(10)):
            pass
        out_err = capsys.readouterr()
        assert out_err.out == ""
        if TQDM_DISABLE is False:  # False but not None
            assert "0/10" in out_err.err
        else:
            assert out_err.err == ""


def test_default(caplog, capsys):
    with caplog.at_level(logging.INFO, logger="dvc"):
        # simulate interactive terminal
        sys.stderr.isatty = lambda: True
        for _ in Tqdm(range(10)):
            pass
        out_err = capsys.readouterr()
        assert out_err.out == ""
        if TQDM_DISABLE:
            assert out_err.err == ""
        else:
            assert "0/10" in out_err.err
