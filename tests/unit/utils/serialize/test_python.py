import pytest

from dvc.utils.serialize import parse_py


@pytest.mark.parametrize(
    "text,result",
    [
        ("BOOL = True", {"BOOL": True}),
        ("INT = 5", {"INT": 5}),
        ("FLOAT = 0.001", {"FLOAT": 0.001}),
        ("STR = 'abc'", {"STR": "abc"}),
        ("DICT = {'a': 1, 'b': 2}", {"DICT": {"a": 1, "b": 2}}),
        ("LIST = [1, 2, 3]", {"LIST": [1, 2, 3]}),
        ("SET = {1, 2, 3}", {"SET": {1, 2, 3}}),
        ("TUPLE = (10, 100)", {"TUPLE": (10, 100)}),
        ("NONE = None", {"NONE": None}),
        ("UNARY_OP = -1", {"UNARY_OP": -1}),
        (
            """class TrainConfig:

            EPOCHS = 70

            def __init__(self):
                self.layers = 5
                self.layers = 9  # TrainConfig.layers param will be 9
                bar = 3  # Will NOT be found since it's locally scoped
            """,
            {"TrainConfig": {"EPOCHS": 70, "layers": 9}},
        ),
    ],
)
def test_parse_valid_types(text, result):
    assert parse_py(text, "foo") == result


@pytest.mark.parametrize(
    "text",
    [
        "CONSTRUCTOR = dict(a=1, b=2)",
        "SUM = 1 + 2",
    ],
)
def test_parse_invalid_types(text):
    assert parse_py(text, "foo") == {}
