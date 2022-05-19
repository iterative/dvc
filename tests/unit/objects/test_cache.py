import pickle
from typing import Any

import pytest

from dvc_objects.cache import Cache, DiskError


def set_value(cache: Cache, key: str, value: Any) -> Any:
    cache[key] = value
    return cache[key]


@pytest.mark.parametrize("disk_type", [None, "test"])
def test_pickle_protocol_error(tmp_path, disk_type):
    directory = tmp_path / "test"
    cache = Cache(
        str(directory),
        disk_pickle_protocol=pickle.HIGHEST_PROTOCOL + 1,
        type=disk_type,
    )
    with pytest.raises(DiskError) as exc, cache as cache:
        set_value(cache, "key", ("value1", "value2"))
    assert exc.value.directory == str(directory)
    assert exc.value.type == "test"
    assert f"Could not open disk 'test' in {directory}" == str(exc.value)


@pytest.mark.parametrize(
    "proto_a, proto_b",
    [
        (pickle.HIGHEST_PROTOCOL - 1, pickle.HIGHEST_PROTOCOL),
        (pickle.HIGHEST_PROTOCOL, pickle.HIGHEST_PROTOCOL - 1),
    ],
)
def test_pickle_backwards_compat(tmp_path, proto_a, proto_b):
    with Cache(
        directory=str(tmp_path / "test"),
        disk_pickle_protocol=proto_a,
    ) as cache:
        set_value(cache, "key", ("value1", "value2"))
    with Cache(
        directory=str(tmp_path / "test"),
        disk_pickle_protocol=proto_b,
    ) as cache:
        assert ("value1", "value2") == cache["key"]
        set_value(cache, "key", ("value3", "value4"))
        assert ("value3", "value4") == cache["key"]
