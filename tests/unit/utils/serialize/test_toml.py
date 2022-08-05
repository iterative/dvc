def test_preserve_comments(tmp_dir):
    from dvc.utils.serialize._toml import modify_toml

    contents_fmt = """\
#A Title
[foo]
bar = {} # meaning of life
baz = [1, 2]
"""
    tmp_dir.gen("params.toml", contents_fmt.format("42"))

    with modify_toml("params.toml") as d:
        d["foo"]["bar"] //= 2
    assert (tmp_dir / "params.toml").read_text() == contents_fmt.format("21")


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
