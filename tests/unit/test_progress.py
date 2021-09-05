import logging

from dvc.progress import Tqdm
from dvc.utils import env2bool


def test_quiet_logging(caplog, capsys):
    with caplog.at_level(logging.CRITICAL, logger="dvc"):
        for _ in Tqdm(range(10)):
            pass
        out_err = capsys.readouterr()
        assert out_err.out == ""
        assert out_err.err == ""


def test_quiet_logging_disable_false(caplog, capsys, mocker):
    # simulate interactive terminal
    mocker.patch("sys.stdout.isatty", return_value=True)
    with caplog.at_level(logging.CRITICAL, logger="dvc"):
        for _ in Tqdm(range(10), disable=False):
            pass
        out_err = capsys.readouterr()
        assert out_err.out == ""
        assert out_err.err == ""


def test_quiet_notty(caplog, capsys):
    with caplog.at_level(logging.INFO, logger="dvc"):
        for _ in Tqdm(range(10)):
            pass
        out_err = capsys.readouterr()
        assert out_err.out == ""
        if env2bool("DVC_IGNORE_ISATTY"):
            assert "0/10" in out_err.err
        else:
            assert out_err.err == ""


def test_default(caplog, capsys, mocker):
    # simulate interactive terminal
    mocker.patch("sys.stdout.isatty", return_value=True)
    with caplog.at_level(logging.INFO, logger="dvc"):
        for _ in Tqdm(range(10)):
            pass

        out_err = capsys.readouterr()
        assert out_err.out == ""
        assert "0/10" in out_err.err
