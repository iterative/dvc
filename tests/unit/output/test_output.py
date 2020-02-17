import pytest

from voluptuous import Schema, MultipleInvalid

from dvc.output import CHECKSUM_SCHEMA


@pytest.mark.parametrize(
    "value,expected",
    [
        ("", None),
        (None, None),
        (11111, "11111"),
        ("11111", "11111"),
        ("aAaBa", "aaaba"),
        (
            "3cc286c534a71504476da009ed174423",
            "3cc286c534a71504476da009ed174423",
        ),  # md5
        (
            "d41d8cd98f00b204e9800998ecf8427e-38",
            "d41d8cd98f00b204e9800998ecf8427e-38",
        ),  # etag
        (
            "000002000000000000000000c16859d1d071c6b1ffc9c8557d4909f1",
            "000002000000000000000000c16859d1d071c6b1ffc9c8557d4909f1",
        ),  # hdfs checksum
        # Not much we can do about hex and oct values without writing our own
        # parser. So listing these test cases just to acknowledge this.
        # See https://github.com/iterative/dvc/issues/3331.
        (0x3451, "13393"),
        (0o1244, "676"),
    ],
)
def test_checksum_schema(value, expected):
    assert Schema(CHECKSUM_SCHEMA)(value) == expected


@pytest.mark.parametrize("value", ["1", "11", {}, {"a": "b"}, [], [1, 2]])
def test_checksum_schema_fail(value):
    with pytest.raises(MultipleInvalid):
        Schema(CHECKSUM_SCHEMA)(value)["md5"]
