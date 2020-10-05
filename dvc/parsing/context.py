import os
from collections import defaultdict
from collections.abc import Mapping, MutableMapping, MutableSequence
from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import Any, List, Optional, Union

from dvc.utils.serialize import LOADERS


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
    # this value is useless for Containers, it's only used to drill down to
    # `Value` for the first time on initialization.
    # Rethink how this should be handled.
    source: Optional[str]
    import_dpath: Optional[str] = None
    imported_as: Optional[str] = None

    dpaths: List[str] = field(default_factory=list)
    aliased: Optional[str] = None

    @staticmethod
    def update_path(meta: "Meta", path):
        if meta.aliased:
            return meta
        dpaths = meta.dpaths[:] + [path]
        return replace(meta, dpaths=dpaths)

    def __str__(self):
        string = self.source or "<local>:"
        if self.import_dpath:
            string += ":"
        string += self.path()
        return string

    def path(self):
        dpath = list(self.dpaths or [])
        if self.import_dpath:
            dpath = [self.import_dpath] + dpath
        return ".".join(dpath)


@dataclass
class Value:
    value: Any
    meta: Optional[Meta] = field(compare=False, default=None, repr=False)

    def __str__(self):
        return str(self.value)


class Container:
    _meta: Meta
    data: Union[list, dict]
    _key_transform = staticmethod(lambda x: x)

    def _convert(self, index, value):
        meta = Meta.update_path(self._meta, index)
        if value is None or isinstance(value, (int, float, str, bytes, bool)):
            return Value(value, meta=meta)
        elif isinstance(value, (CtxList, CtxDict, Value)):
            return value
        elif isinstance(value, (list, dict)):
            container = CtxDict if isinstance(value, dict) else CtxList
            return container(value, meta=meta)
        else:
            msg = "Unsupported value of type '{value}' in '{meta}'"
            raise TypeError(msg)

    def __setitem__(self, index, value):
        self.data[index] = self._convert(index, value)

    def __repr__(self):
        return "".join(["<", self.__class__.__name__, ": ", str(self), ">"])

    def __str__(self):
        return str(self.data)

    def __eq__(self, o):
        return o.data == self.data

    def __getitem__(self, k):
        return self.data[k]

    def __delitem__(self, v):
        del self.data[v]

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def select(self, key: str):
        index, *rems = key.split(sep=".", maxsplit=1)
        index = self._key_transform(index)
        try:
            d = self.data[index]
        except LookupError as exc:
            raise ValueError(
                f"Could not find '{index}' in {self.data}"
            ) from exc

        return d.select(rems[0]) if rems else d


class CtxList(Container, MutableSequence):

    _key_transform = staticmethod(int)

    def __init__(self, values, meta, **kwargs):
        self._meta = meta
        self.data: list = []
        self.extend(values)

    def insert(self, index: int, value):
        self.data.insert(index, self._convert(index, value))


class CtxDict(Container, MutableMapping):
    def __init__(self, mapping, meta, **kwargs):
        self._meta = meta
        mapping = mapping or {}
        mapping.update(**kwargs)
        self.data: dict = {}
        self.update(mapping)

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            # limitations for the interpolation ignore other kinds of keys
            return
        return super().__setitem__(key, value)

    def merge_update(self, *args, overwrite=True):
        for d in args:
            _merge(self.data, d, overwrite=overwrite)


class Context(Container):
    def __init__(self, data: Union[CtxDict, CtxList]):
        self.data: Union[CtxDict, CtxList] = data
        self._meta = Meta(source=None)
        self._tracked_data = defaultdict(list)

    @classmethod
    def create(cls, d, source):
        return cls(cls._make_context(d, Meta(source=source)))

    @classmethod
    def _make_context(cls, data, meta):
        if isinstance(data, (CtxDict, CtxList)):
            return data

        if not isinstance(data, (list, dict)):
            raise ValueError(
                "Does not support loading types other than list or dict"
            )
        container = CtxDict if isinstance(data, dict) else CtxList
        return container(data, meta=meta)

    @classmethod
    def load_variables(cls, imports: List[str], constants):
        d = map(cls._load_and_select, imports)
        m = cls._make_context(constants, Meta(source=None))
        m.merge_update(*d)
        return cls(m)

    @classmethod
    def clone(cls, ctx):
        """Clones given context."""
        return cls(deepcopy(ctx.data))

    @classmethod
    def _load(cls, file: str):
        return LOADERS[os.path.splitext(file)[1]](file)

    @classmethod
    def _load_and_select(cls, imp: str):
        file, *dpath = imp.rsplit(":", maxsplit=1)
        key = dpath[0] if dpath else None
        # TODO: normalize file path before passing it to the Meta
        # Note that `source` should be relative to the path of the
        # dvc.yaml file rather than the pwd.
        meta = Meta(source=file, imported_as=imp, import_dpath=key)
        d = cls._make_context(cls._load(file), meta=meta)
        return d.select(dpath[0]) if dpath else d

    def select(
        self, key: str, track=False
    ):  # pylint: disable=arguments-differ
        node = self.data.select(key)
        if isinstance(node, Value):
            meta = node.meta
            if meta and meta.source and track:
                self._tracked_data[meta.source].append(meta.path())
        return node

    @property
    def tracked(self):
        return [{file: keys} for file, keys in self._tracked_data.items()]

    def merge_update(self, *args, overwrite=False):
        for ctx in args:
            _merge(self.data, ctx.data, overwrite=overwrite)
