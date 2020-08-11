import pytest

from dvc.exceptions import YAMLFileCorruptedError
from dvc.utils.yaml import (
    YAMLVersion,
    dump_yaml,
    parse_yaml,
    parse_yaml_for_update,
)

V12 = YAMLVersion.V12
V11 = YAMLVersion.V11


def _get_directive(version):
    return "%YAML {ver}\n---\n".format(
        ver=".".join(str(num) for num in version)
    )


@pytest.mark.parametrize("data", [{"x": 3e24}])
@pytest.mark.parametrize("with_directive", [True, False])
@pytest.mark.parametrize(
    "ver, directive, expected",
    [
        # dot before mantissa is not required in yaml1.2,
        # whereas it's required in yaml1.1
        (V12, _get_directive(V12), "x: 3e+24\n"),
        (V11, _get_directive(V11), "x: 3.0e+24\n"),
    ],
)
def test_dump_yaml_with_directive(
    tmp_dir, ver, directive, expected, with_directive, data
):
    dump_yaml("data.yaml", data, version=ver, with_directive=with_directive)
    actual = (tmp_dir / "data.yaml").read_text()
    exp = expected if not with_directive else directive + expected
    assert actual == exp


def test_load_yaml():
    assert parse_yaml("x: 3e24", "data.yaml") == {"x": "3e24"}
    assert parse_yaml("x: 3.0e+24", "data.yaml") == {"x": 3e24}

    assert parse_yaml("x: 3e24", "data.yaml", version=V12) == {"x": 3e24}
    assert parse_yaml("x: 3.0e+24", "data.yaml", version=V12) == {"x": 3e24}

    with pytest.raises(YAMLFileCorruptedError):
        assert parse_yaml("invalid: '", "data.yaml")

    with pytest.raises(YAMLFileCorruptedError):
        assert parse_yaml("invalid: '", "data.yaml", version=V12)


def test_comments_are_preserved_on_update_and_dump(tmp_dir):
    text = "x: 3  # this is a comment"
    d = parse_yaml_for_update(text, "data.yaml")
    d["w"] = 7e24

    dump_yaml("data.yaml", d)
    assert (tmp_dir / "data.yaml").read_text() == f"{text}\nw: 7.0e+24\n"

    dump_yaml("data.yaml", d, with_directive=True)
    assert (tmp_dir / "data.yaml").read_text() == _get_directive(
        V11
    ) + f"{text}\nw: 7.0e+24\n"

    dump_yaml("data.yaml", d, version=V12)
    assert (tmp_dir / "data.yaml").read_text() == f"{text}\nw: 7e+24\n"

    dump_yaml("data.yaml", d, with_directive=True, version=V12)
    assert (tmp_dir / "data.yaml").read_text() == _get_directive(
        V12
    ) + f"{text}\nw: 7e+24\n"
