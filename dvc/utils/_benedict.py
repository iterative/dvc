"""
Rollbacks monkeypatching of `json.encoder` by benedict.

It monkeypatches json to use Python-based encoder instead of C-based
during import.

We rollback that monkeypatch by keeping reference to that C-based
encoder and reinstate them after importing benedict.
See: https://github.com/iterative/dvc/issues/6423
     https://github.com/fabiocaccamo/python-benedict/issues/62
and the source of truth:
https://github.com/fabiocaccamo/python-benedict/blob/c98c471065/benedict/dicts/__init__.py#L282-L285
"""
from json import encoder

try:
    c_make_encoder = encoder.c_make_encoder  # type: ignore[attr-defined]
except AttributeError:
    c_make_encoder = None


from benedict import benedict  # noqa: E402

encoder.c_make_encoder = c_make_encoder  # type: ignore[attr-defined]
# Please import benedict from here lazily
__all__ = ["benedict"]
