from typing import TYPE_CHECKING, Any
from unittest.mock import ANY

if TYPE_CHECKING:
    from unittest.mock import Mock


def issubset(subset: dict, superset: dict) -> bool:
    assert superset == superset | subset
    return True


def called_once_with_subset(m: "Mock", *args: Any, **kwargs: Any) -> bool:
    m.assert_called_once()
    m_args, m_kwargs = m.call_args

    expected_args = m_args + (ANY,) * (len(m_args) - len(args) - 1)
    expected_kwargs = {k: kwargs.get(k, ANY) for k in m_kwargs}
    m.assert_called_with(*expected_args, **expected_kwargs)

    return True
