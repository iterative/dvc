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
V12_DIRECTIVE = "%YAML 1.2\n---\n"
V11_DIRECTIVE = "%YAML 1.1\n---\n"


@pytest.mark.parametrize("data", [{"x": 3e24}])
@pytest.mark.parametrize("with_directive", [True, False])
@pytest.mark.parametrize(
    "ver, directive, expected",
    [
        # dot before mantissa is not required in yaml1.2,
        # whereas it's required in yaml1.1
        (V12, V12_DIRECTIVE, "x: 3e+24\n"),
        (V11, V11_DIRECTIVE, "x: 3.0e+24\n"),
    ],
)
def test_dump_yaml_with_directive(
    tmp_dir, ver, directive, expected, with_directive, data
):
    dump_yaml("data.yaml", data, version=ver, with_directive=with_directive)
    actual = (tmp_dir / "data.yaml").read_text()
    exp = expected if not with_directive else directive + expected
    assert actual == exp


@pytest.mark.parametrize(
    "parser, rt_parser", [(parse_yaml, False), (parse_yaml_for_update, True)]
)
def test_load_yaml(parser, rt_parser):
    # ruamel.yaml.load() complains about dot before mantissa not allowed
    # on 1.1 and goes on anyway to convert this to a float based on 1.2 spec
    str_value = "3e24"
    float_value = float(str_value)
    yaml11_text = "x: 3.0e+24"  # pyyaml parses as str if there's no +/- sign
    # luckily, `ruamel.yaml` always dumps with sign
    yaml12_text = f"x: {str_value}"
    assert parser(yaml11_text, "data.yaml") == {"x": float_value}
    assert parser(yaml12_text, "data.yaml") == {
        "x": float_value if rt_parser else str_value
    }

    assert parser(yaml11_text, "data.yaml", version=V12) == {"x": float_value}
    assert parser(yaml12_text, "data.yaml", version=V12) == {"x": float_value}

    with pytest.raises(YAMLFileCorruptedError):
        assert parser("invalid: '", "data.yaml")

    with pytest.raises(YAMLFileCorruptedError):
        assert parser("invalid: '", "data.yaml", version=V12)


def test_comments_are_preserved_on_update_and_dump(tmp_dir):
    text = "x: 3  # this is a comment"
    d = parse_yaml_for_update(text, "data.yaml")
    d["w"] = 7e24

    dump_yaml("data.yaml", d)
    assert (tmp_dir / "data.yaml").read_text() == f"{text}\nw: 7.0e+24\n"

    dump_yaml("data.yaml", d, with_directive=True)
    assert (
        tmp_dir / "data.yaml"
    ).read_text() == V11_DIRECTIVE + f"{text}\nw: 7.0e+24\n"

    dump_yaml("data.yaml", d, version=V12)
    assert (tmp_dir / "data.yaml").read_text() == f"{text}\nw: 7e+24\n"

    dump_yaml("data.yaml", d, with_directive=True, version=V12)
    assert (
        tmp_dir / "data.yaml"
    ).read_text() == V12_DIRECTIVE + f"{text}\nw: 7e+24\n"
