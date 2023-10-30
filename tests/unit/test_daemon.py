import inspect
import os

from dvc import daemon


def test_daemon(mocker):
    mock = mocker.patch("dvc.daemon._spawn")
    daemon.daemon(["updater"])

    mock.assert_called()
    args = mock.call_args[0]
    env = args[2]
    assert "PYTHONPATH" in env

    file_path = os.path.abspath(inspect.stack()[0][1])
    file_dir = os.path.dirname(file_path)
    test_dir = os.path.dirname(file_dir)
    dvc_dir = os.path.dirname(test_dir)
    assert env["PYTHONPATH"] == dvc_dir
    assert env[daemon.DVC_DAEMON] == "1"


def test_no_recursive_spawn(mocker):
    mocker.patch.dict(os.environ, {daemon.DVC_DAEMON: "1"})
    mock_spawn = mocker.patch("dvc.daemon._spawn")
    daemon.daemon(["updater"])
    mock_spawn.assert_not_called()
