import pickle
from typing import Any

import diskcache
import pytest

from dvc.exceptions import DvcException
from dvc.utils.decorators import with_diskcache


@with_diskcache(name="test")
def set_value(cache: diskcache.Cache, key: str, value: Any) -> Any:
    cache[key] = value
    return cache[key]


def test_pickle_protocol_error(tmp_dir):
    with pytest.raises(DvcException) as exc:
        with diskcache.Cache(
            directory=(tmp_dir / "test"),
            disk_pickle_protocol=pickle.HIGHEST_PROTOCOL + 1,
        ) as cache:
            set_value(cache, "key", ("value1", "value2"))
        assert "troubleshooting#pickle" in str(exc)


@pytest.mark.parametrize(
    "proto_a, proto_b",
    [
        (pickle.HIGHEST_PROTOCOL - 1, pickle.HIGHEST_PROTOCOL),
        (pickle.HIGHEST_PROTOCOL, pickle.HIGHEST_PROTOCOL - 1),
    ],
)
def test_pickle_backwards_compat(tmp_dir, proto_a, proto_b):
    with diskcache.Cache(
        directory=(tmp_dir / "test"),
        disk_pickle_protocol=proto_a,
    ) as cache:
        set_value(cache, "key", ("value1", "value2"))
    with diskcache.Cache(
        directory=(tmp_dir / "test"),
        disk_pickle_protocol=proto_b,
    ) as cache:
        assert ("value1", "value2") == cache["key"]
        set_value(cache, "key", ("value3", "value4"))
        assert ("value3", "value4") == cache["key"]
