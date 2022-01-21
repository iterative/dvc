from dvc.cli import main


def test_root(tmp_dir, dvc, capsys):
    assert main(["root"]) == 0
    assert "." in capsys.readouterr()[0]


def test_root_locked(tmp_dir, dvc, capsys):
    # NOTE: check that `dvc root` is not blocked with dvc lock
    with dvc.lock:
        assert main(["root"]) == 0
    assert "." in capsys.readouterr()[0]
