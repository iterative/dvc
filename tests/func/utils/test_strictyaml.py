import os


def test_make_relpath(tmp_dir, monkeypatch):
    from dvc.utils.strictyaml import make_relpath

    path = tmp_dir / "dvc.yaml"
    expected_path = "./dvc.yaml" if os.name == "posix" else ".\\dvc.yaml"
    assert make_relpath(path) == expected_path

    (tmp_dir / "dir").mkdir(exist_ok=True)
    monkeypatch.chdir("dir")

    expected_path = "../dvc.yaml" if os.name == "posix" else "..\\dvc.yaml"
    assert make_relpath(path) == expected_path
