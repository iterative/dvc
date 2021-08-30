import pytest

from dvc.utils.serialize import (
    EncodingError,
    YAMLFileCorruptedError,
    load_yaml,
    parse_yaml,
)


def test_parse_yaml_duplicate_key_error():
    text = """\
    mykey:
    - foo
    mykey:
    - bar
    """
    with pytest.raises(YAMLFileCorruptedError):
        parse_yaml(text, "mypath")


def test_parse_yaml_invalid_unicode(tmp_dir):
    filename = "invalid_utf8.yaml"
    tmp_dir.gen(filename, b"\x80some: stuff")

    with pytest.raises(EncodingError) as excinfo:
        load_yaml(tmp_dir / filename)

    assert filename in excinfo.value.path
    assert excinfo.value.encoding == "utf-8"
