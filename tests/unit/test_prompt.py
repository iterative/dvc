from dvc.prompt import confirm


def test_confirm_in_tty_if_stdin_is_closed(mocker):
    mock_input = mocker.patch("dvc.prompt.input", side_effect=EOFError)
    mock_isatty = mocker.patch("sys.stdout.isatty", return_value=True)
    ret = confirm("message")
    mock_isatty.assert_called()
    mock_input.assert_called()
    assert not ret
