import inspect
import os

from dvc import daemon


def test_daemon(mocker):
    mock_windows = mocker.patch("dvc.daemon._spawn_windows")
    mock_posix = mocker.patch("dvc.daemon._spawn_posix")
    daemon.daemon(["updater"])

    if os.name == "nt":
        mock_posix.assert_not_called()
        mock_windows.assert_called()
        args = mock_windows.call_args[0]
    else:
        mock_windows.assert_not_called()
        mock_posix.assert_called()
        args = mock_posix.call_args[0]

    env = args[1]
    assert "PYTHONPATH" in env.keys()

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
