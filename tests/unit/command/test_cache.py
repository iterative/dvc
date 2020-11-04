from dvc.cli import parse_args


def test_default(mocker, caplog):
    args = parse_args(["cache", "status"])
    cmd = args.func(args)

    mocker.patch("os.access", return_value=True)

    assert 0 == cmd.run()
    assert "Step 1: permissions check on:" in caplog.text
    assert "Read: OK" in caplog.text
    assert "Write: OK" in caplog.text
    assert "Exist: OK" in caplog.text
    assert "Step 2: cache details of all DVC-tracked files:" in caplog.text
