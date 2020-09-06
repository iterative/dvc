from dvc.main import main


def test_(tmp_dir, dvc, scm):
    assert main(["version"]) == 0
