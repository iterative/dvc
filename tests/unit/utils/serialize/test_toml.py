def test_preserve_comments(tmp_dir, dvc):
    from dvc.utils.serialize import _toml

    contents = "# A Title [foo]\nbar = 42# meaning of life\nbaz = [1, 2]\n"
    tmp_dir.gen("params_commented.toml", "")
    path = (tmp_dir / "params_commented.toml").fs_path

    parsed = _toml.parse_toml_for_update(contents, path)
    with open(path, "w", encoding="utf-8") as fobj:
        _toml._dump(parsed, fobj)

    with open(path, "r", encoding="utf-8") as fobj:
        new_contents = fobj.read()

    assert new_contents == contents


def test_parse_toml_type():
    from tomlkit.toml_document import TOMLDocument

    from dvc.utils.serialize._toml import parse_toml

    contents = "# A Title [foo]\nbar = 42# meaning of life\nbaz = [1, 2]\n"

    parsed = parse_toml(contents, ".")
    assert not isinstance(parsed, TOMLDocument)
    assert isinstance(parsed, dict)


def test_parse_toml_for_update():
    from tomlkit.toml_document import TOMLDocument

    from dvc.utils.serialize._toml import parse_toml_for_update

    contents = "# A Title [foo]\nbar = 42# meaning of life\nbaz = [1, 2]\n"

    parsed = parse_toml_for_update(contents, ".")
    assert isinstance(parsed, TOMLDocument)
    assert isinstance(parsed, dict)
