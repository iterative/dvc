from unittest import mock

from dvc.prompt import confirm


def test_confirm_in_tty_if_stdin_is_closed():
    with mock.patch("dvc.prompt.input", side_effect=EOFError) as mock_input:
        with mock.patch("sys.stdout.isatty", return_value=True) as mock_isatty:
            ret = confirm("message")
            mock_isatty.assert_called()
            mock_input.assert_called()
            assert not ret
