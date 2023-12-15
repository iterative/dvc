import pytest

from dvc.utils.serialize import ConfigFileCorruptedError


def test_update(tmp_dir):
    from dvc.utils.serialize._ini import modify_ini

    contents_fmt = """\
#A Title
[section.foo]
bar = 42
baz = [1, 2]
ref = ${section.foo.bar}

[section.foo."bar.baz".qux]
value = 42
"""
    tmp_dir.gen("params.ini", contents_fmt)

    with modify_ini("params.ini") as d:
        d["section"]["foo"]["bar"] //= 2
    assert (
        (tmp_dir / "params.ini").read_text()
        == """\
[section.foo]
bar = 21
baz = [1, 2]
ref = ${section.foo.bar}

[section.foo.'bar.baz'.qux]
value = 42

"""
    )


def test_parse_error():
    from dvc.utils.serialize._ini import parse_ini

    contents = "# A Title [foo]\nbar = 42# meaning of life\nbaz = [1, 2]\n"

    with pytest.raises(ConfigFileCorruptedError):
        parse_ini(contents, ".")


def test_split_path():
    from dvc.utils.serialize._ini import split_path

    assert split_path("foo.bar") == ["foo", "bar"]
    assert split_path('foo."bar.test"') == ["foo", "bar.test"]
