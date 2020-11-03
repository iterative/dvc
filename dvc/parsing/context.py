import os
from collections import defaultdict
from collections.abc import Mapping, MutableMapping, MutableSequence, Sequence
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import Any, List, Optional, Union

from funcy import identity

from dvc.parsing.interpolate import (
    UNWRAP_DEFAULT,
    get_matches,
    is_exact_string,
    is_interpolated_string,
    resolve_str,
)
from dvc.utils.serialize import LOADERS

SeqOrMap = Union[Sequence, Mapping]


def _merge(into, update, overwrite):
    for key, val in update.items():
        if isinstance(into.get(key), Mapping) and isinstance(val, Mapping):
            _merge(into[key], val, overwrite)
        else:
            if key in into and not overwrite:
                raise ValueError(
                    f"Cannot overwrite as key {key} already exists in {into}"
                )
            into[key] = val


@dataclass
class Meta:
    source: Optional[str] = None
    dpaths: List[str] = field(default_factory=list)

    @staticmethod
    def update_path(meta: "Meta", path: Union[str, int]):
        dpaths = meta.dpaths[:] + [str(path)]
        return replace(meta, dpaths=dpaths)

    def __str__(self):
        string = self.source or "<local>:"
        string += self.path()
        return string

    def path(self):
        return ".".join(self.dpaths)


def _default_meta():
    return Meta(source=None)


@dataclass
class Value:
    value: Any
    meta: Meta = field(
        compare=False, default_factory=_default_meta, repr=False
    )

    def __repr__(self):
        return repr(self.value)

    def __str__(self) -> str:
        return str(self.value)

    def get_sources(self):
        return {self.meta.source: self.meta.path()}


class Container:
    meta: Meta
    data: Union[list, dict]
    _key_transform = staticmethod(identity)

    def __init__(self, meta=None) -> None:
        self.meta = meta or _default_meta()

    def _convert(self, key, value):
        meta = Meta.update_path(self.meta, key)
        if value is None or isinstance(value, (int, float, str, bytes, bool)):
            return Value(value, meta=meta)
        elif isinstance(value, (CtxList, CtxDict, Value)):
            return value
        elif isinstance(value, (list, dict)):
            container = CtxDict if isinstance(value, dict) else CtxList
            return container(value, meta=meta)
        else:
            msg = (
                "Unsupported value of type "
                f"'{type(value).__name__}' in '{meta}'"
            )
            raise TypeError(msg)

    def __repr__(self):
        return repr(self.data)

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = self._convert(key, value)

    def __delitem__(self, key):
        del self.data[key]

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __eq__(self, o):
        container = type(self)
        if isinstance(o, container):
            return o.data == self.data
        return container(o) == self

    def select(self, key: str):
        index, *rems = key.split(sep=".", maxsplit=1)
        index = index.strip()
        index = self._key_transform(index)
        try:
            d = self.data[index]
        except LookupError as exc:
            raise ValueError(
                f"Could not find '{index}' in {self.data}"
            ) from exc
        return d.select(rems[0]) if rems else d

    def get_sources(self):
        return {}


class CtxList(Container, MutableSequence):
    _key_transform = staticmethod(int)

    def __init__(self, values: Sequence, meta: Meta = None):
        super().__init__(meta=meta)
        self.data: list = []
        self.extend(values)

    def insert(self, index: int, value):
        self.data.insert(index, self._convert(index, value))

    def get_sources(self):
        return {self.meta.source: self.meta.path()}


class CtxDict(Container, MutableMapping):
    def __init__(self, mapping: Mapping = None, meta: Meta = None, **kwargs):
        super().__init__(meta=meta)

        self.data: dict = {}
        if mapping:
            self.update(mapping)
        self.update(kwargs)

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            # limitation for the interpolation
            # ignore other kinds of keys
            return
        return super().__setitem__(key, value)

    def merge_update(self, *args, overwrite=False):
        for d in args:
            _merge(self.data, d, overwrite=overwrite)


class Context(CtxDict):
    def __init__(self, *args, **kwargs):
        """
        Top level mutable dict, with some helpers to create context and track
        """
        super().__init__(*args, **kwargs)
        self._track = False
        self._tracked_data = defaultdict(set)

    @contextmanager
    def track(self):
        self._track = True
        yield
        self._track = False

    def _track_data(self, node):
        if not self._track:
            return

        for source, keys in node.get_sources().items():
            if not source:
                continue
            params_file = self._tracked_data[source]
            keys = [keys] if isinstance(keys, str) else keys
            params_file.update(keys)

    @property
    def tracked(self):
        return self._tracked_data

    def select(self, key: str):
        node = super().select(key)
        self._track_data(node)
        return node

    @classmethod
    def load_from(cls, tree, file: str) -> "Context":
        _, ext = os.path.splitext(file)
        loader = LOADERS[ext]

        meta = Meta(source=file)
        return cls(loader(file, tree=tree), meta=meta)

    @classmethod
    def clone(cls, ctx: "Context") -> "Context":
        """Clones given context."""
        return cls(deepcopy(ctx.data))

    def set(self, key, value):
        """
        Sets a value, either non-interpolated values to a key,
        or an interpolated string after resolving it.

        >>> c = Context({"foo": "foo", "bar": [1, 2], "lorem": {"a": "z"}})
        >>> c.set("foobar", "${bar}")
        >>> c
        {'foo': 'foo', 'bar': [1, 2], 'lorem': {'a': 'z'}, 'foobar': [1, 2]}
        """
        if key in self:
            raise ValueError(f"Cannot set '{key}', key already exists")
        if isinstance(value, str):
            self._check_joined_with_interpolation(key, value)
            value = self.resolve_str(value, unwrap=False)
        elif isinstance(value, (Sequence, Mapping)):
            self._check_not_nested_collection(key, value)
            self._check_interpolation_collection(key, value)
        self[key] = value

    def resolve(self, src):
        """Recursively resolves interpolation and returns resolved data.

        >>> c = Context({"three": 3})
        >>> c.resolve({"lst": [1, 2, "${three}"]})
        {'lst': [1, 2, 3]}
        """
        Seq = (list, tuple, set)
        if isinstance(src, Mapping):
            return {key: self.resolve(value) for key, value in src.items()}
        elif isinstance(src, Seq):
            return type(src)(map(self.resolve, src))
        elif isinstance(src, str):
            return self.resolve_str(src)
        return src

    def resolve_str(self, src, unwrap=UNWRAP_DEFAULT):
        """Resolves interpolated string to it's original value,
        or in case of multiple interpolations, a combined string.

        >>> c = Context({"enabled": True})
        >>> c.resolve_str("${enabled}")
        True
        >>> c.resolve_str("enabled? ${enabled}")
        'enabled? True'
        """
        return resolve_str(src, self, unwrap=unwrap)

    @staticmethod
    def _check_not_nested_collection(key: str, value: SeqOrMap):
        values = value.values() if isinstance(value, Mapping) else value
        has_nested = any(
            not isinstance(item, str) and isinstance(item, (Mapping, Sequence))
            for item in values
        )
        if has_nested:
            raise ValueError(f"Cannot set '{key}', has nested dict/list")

    @staticmethod
    def _check_interpolation_collection(key: str, value: SeqOrMap):
        values = value.values() if isinstance(value, Mapping) else value
        interpolated = any(is_interpolated_string(item) for item in values)
        if interpolated:
            raise ValueError(
                f"Cannot set '{key}', "
                "having interpolation inside "
                f"'{type(value).__name__}' is not supported."
            )

    @staticmethod
    def _check_joined_with_interpolation(key: str, value: str):
        matches = get_matches(value)
        if matches and not is_exact_string(value, matches):
            raise ValueError(
                f"Cannot set '{key}', "
                "joining string with interpolated string"
                "is not supported"
            )


if __name__ == "__main__":
    import doctest

    doctest.testmod()
