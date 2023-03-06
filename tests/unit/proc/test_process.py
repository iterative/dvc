import json
import subprocess

import pytest

from dvc.proc.process import ManagedProcess, ProcessInfo

TEST_PID = 1234


@pytest.fixture(autouse=True)
def mock_popen(mocker):
    mocker.patch(
        "subprocess.Popen",
        return_value=mocker.MagicMock(pid=TEST_PID, returncode=None),
    )


@pytest.mark.parametrize(
    "args",
    [
        "/bin/foo -o option",
        ["/bin/foo", "-o", "option"],
    ],
)
def test_init_args(tmp_dir, request, args, mocker):
    expected = ["/bin/foo", "-o", "option"]
    proc = ManagedProcess(args)
    request.addfinalizer(proc._close_fds)
    assert expected == proc.args


def test_run(tmp_dir, request, mocker):
    proc = ManagedProcess("/bin/foo")

    request.addfinalizer(proc._close_fds)
    assert proc.pid == TEST_PID

    with open(proc.info_path, encoding="utf-8") as fobj:
        info = ProcessInfo.from_dict(json.load(fobj))
        assert info.pid == TEST_PID


def test_wait(tmp_dir, mocker):
    from dvc.proc.exceptions import TimeoutExpired

    proc = ManagedProcess("/bin/foo")
    proc._proc.wait = mocker.Mock(side_effect=subprocess.TimeoutExpired("/bin/foo", 5))
    with pytest.raises(TimeoutExpired):
        proc.wait(5)

    proc._proc.wait = mocker.Mock(return_value=None)
    proc._proc.returncode = 0
    assert proc.wait() == 0
    proc._proc.wait.assert_called_once()

    # once subprocess return code is set, future ManagedProcess.wait() calls
    # should not block
    proc._proc.wait.reset_mock()
    assert proc.wait() == 0
    proc._proc.wait.assert_not_called()
