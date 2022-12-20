from typing import Any, Dict


def issubset(subset: Dict, superset: Dict) -> bool:
    assert {**superset, **subset} == superset
    return True


def called_once_with_subset(mocker, m, *args: Any, **kwargs: Any) -> bool:
    m.assert_called_once()
    m_args, m_kwargs = m.call_args

    expected_args = m_args + (mocker.ANY,) * (len(m_args) - len(args) - 1)
    expected_kwargs = {k: kwargs.get(k, mocker.ANY) for k in m_kwargs}
    m.assert_called_with(*expected_args, **expected_kwargs)

    return True
