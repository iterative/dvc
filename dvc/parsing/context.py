from collections.abc import Collection, Mapping, Sequence

# for testing purpose
# FIXME: after implementing of reading of "params".
TEST_DATA = {
    "__test__": {
        "dict": {"one": 1, "two": 2, "three": "three", "four": "4"},
        "list": [1, 2, 3, 4, 3.14],
        "set": {1, 2, 3},
        "tuple": (1, 2),
        "bool": True,
        "none": None,
        "float": 3.14,
        "nomnom": 1000,
    }
}


class Context:
    def __init__(self, data=None):
        self.data = data or TEST_DATA

    def select(self, key):
        return _get_value(self.data, key)


def _get_item(data, idx):
    if isinstance(data, Sequence):
        idx = int(idx)

    if isinstance(data, (Mapping, Sequence)):
        return data[idx]

    raise ValueError(
        f"Cannot get item '{idx}' from data of type '{type(data).__name__}'"
    )


def _get_value(data, key):
    obj_and_attrs = key.strip().split(".")
    value = data
    for attr in obj_and_attrs:
        if attr == "":
            raise ValueError("Syntax error!")

        try:
            value = _get_item(value, attr)
        except KeyError:
            msg = (
                f"Could not find '{attr}' "
                "while substituting "
                f"'{key}'.\n"
                f"Interpolating with: {data}"
            )
            raise ValueError(msg)

    if not isinstance(value, str) and isinstance(value, Collection):
        raise ValueError(
            f"Cannot interpolate value of type '{type(value).__name__}'"
        )
    return value
