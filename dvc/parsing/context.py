import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Mapping, MutableMapping, MutableSequence, Sequence
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import Any, List, Optional, Union

from funcy import identity, lfilter

from dvc.exceptions import DvcException
from dvc.parsing.interpolate import (
    get_expression,
    get_matches,
    is_exact_string,
    normalize_key,
    recurse,
    str_interpolate,
)
from dvc.path_info import PathInfo
from dvc.utils import relpath
from dvc.utils.serialize import LOADERS

logger = logging.getLogger(__name__)
SeqOrMap = Union[Sequence, Mapping]


class ContextError(DvcException):
    pass


class MergeError(ContextError):
    def __init__(self, key, new, into):
        self.key = key
        if not isinstance(into[key], Node) or not isinstance(new, Node):
            super().__init__(
                f"cannot merge '{key}' as it already exists in {into}"
            )
            return

        preexisting = into[key].meta.source
        new_src = new.meta.source
        path = new.meta.path()
        super().__init__(
            f"cannot redefine '{path}' from '{new_src}'"
            f" as it already exists in '{preexisting}'"
        )


class ParamsFileNotFound(ContextError):
    pass


class KeyNotInContext(ContextError):
    def __init__(self, key) -> None:
        self.key = key
        super().__init__(f"Could not find '{key}'")


class VarsAlreadyLoaded(ContextError):
    pass


def _merge(into, update, overwrite):
    for key, val in update.items():
        if isinstance(into.get(key), Mapping) and isinstance(val, Mapping):
            _merge(into[key], val, overwrite)
        else:
            if key in into and not overwrite:
                raise MergeError(key, val, into)
            into[key] = val
            assert isinstance(into[key], Node)


def recurse_not_a_node(data: dict):
    def func(item):
        assert not isinstance(item, Node)

    return recurse(func)(data)


@dataclass
class Meta:
    source: Optional[str] = None
    dpaths: List[str] = field(default_factory=list)
    local: bool = True

    @staticmethod
    def update_path(meta: "Meta", path: Union[str, int]):
        dpaths = meta.dpaths[:] + [str(path)]
        return replace(meta, dpaths=dpaths)

    def __str__(self):
        string = self.source or "<local>"
        string += ":" + self.path()
        return string

    def path(self):
        return ".".join(self.dpaths)


def _default_meta():
    return Meta(source=None)


class Node:
    def get_sources(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def value(self):
        pass


@dataclass
class Value(Node):
    _value: Any
    meta: Meta = field(
        compare=False, default_factory=_default_meta, repr=False
    )

    def __repr__(self):
        return repr(self._value)

    def __str__(self) -> str:
        return str(self._value)

    def get_sources(self):
        return {self.meta.source: self.meta.path()}

    @property
    def value(self):
        return self._value


PRIMITIVES = (int, float, str, bytes, bool)


class Container(Node, ABC):
    meta: Meta
    data: Union[list, dict]
    _key_transform = staticmethod(identity)

    def __init__(self, meta=None) -> None:
        self.meta = meta or _default_meta()

    def _convert(self, key, value):
        meta = Meta.update_path(self.meta, key)
        return self._convert_with_meta(value, meta)

    @staticmethod
    def _convert_with_meta(value, meta: Meta = None):
        if value is None or isinstance(value, PRIMITIVES):
            assert meta
            return Value(value, meta=meta)
        elif isinstance(value, Node):
            return value
        elif isinstance(value, (list, dict)):
            assert meta
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
            d = self[index]
        except LookupError as exc:
            raise ValueError(
                f"Could not find '{index}' in {self.data}"
            ) from exc

        if not rems:
            return d

        rem = rems[0]
        if not isinstance(d, Container):
            raise ValueError(
                f"{index} is a primitive value, cannot get '{rem}'"
            )
        return d.select(rem)

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

    @property
    def value(self):
        return [node.value for node in self]

    def __deepcopy__(self, _):
        # optimization: we don't support overriding a list
        new = CtxList([])
        new.data = self.data[:]  # shortcircuting __setitem__
        return new


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
            _merge(self, d, overwrite=overwrite)

    @property
    def value(self):
        return {key: node.value for key, node in self.items()}

    def __deepcopy__(self, _):
        new = CtxDict()
        for k, v in self.items():
            new.data[k] = (
                deepcopy(v) if isinstance(v, Container) else v
            )  # shortcircuting __setitem__
        return new


class Context(CtxDict):
    def __init__(self, *args, **kwargs):
        """
        Top level mutable dict, with some helpers to create context and track
        """
        super().__init__(*args, **kwargs)
        self._track = False
        self._tracked_data = defaultdict(dict)
        self.imports = {}

    @contextmanager
    def track(self):
        self._track = True
        yield self._tracked_data

        self._track = False
        self._tracked_data = defaultdict(dict)

    def _track_data(self, node):
        if not self._track:
            return

        if node.meta and node.meta.local:
            return

        for source, keys in node.get_sources().items():
            if not source:
                continue
            params_file = self._tracked_data[source]
            keys = [keys] if isinstance(keys, str) else keys
            params_file.update({key: node.value for key in keys})

    def select(
        self, key: str, unwrap=False
    ):  # pylint: disable=arguments-differ
        """Select the item using key, similar to `__getitem__`
           but can track the usage of the data on interpolation
           as well and can get from nested data structure by using
           "." separated key (eg: "key1.key2.key3")

        Args:
            key: key to select value from
            unwrap: Convert CtxList/CtxDict/Value items to it's original data
                    Defaults to False. Note that the default is different from
                    `resolve`.
        """
        key = normalize_key(key)
        try:
            node = super().select(key)
        except ValueError as exc:
            raise KeyNotInContext(key) from exc

        assert isinstance(node, Node)
        self._track_data(node)
        return node.value if unwrap else node

    @classmethod
    def load_from(cls, tree, path: PathInfo, select_keys=None) -> "Context":
        file = relpath(path)
        if not tree.exists(path):
            raise ParamsFileNotFound(f"'{file}' does not exist")

        _, ext = os.path.splitext(file)
        loader = LOADERS[ext]

        data = loader(path, tree=tree)
        select_keys = select_keys or []
        if select_keys:
            try:
                data = {key: data[key] for key in select_keys}
            except KeyError as exc:
                key, *_ = exc.args
                raise ContextError(
                    f"could not find '{key}' in '{file}'"
                ) from exc

        meta = Meta(source=file, local=False)
        ctx = cls(data, meta=meta)
        ctx.imports[os.path.abspath(path)] = select_keys or None
        return ctx

    def merge_from(
        self, tree, item: str, wdir: PathInfo, overwrite=False,
    ):
        path, _, keys_str = item.partition(":")
        select_keys = lfilter(bool, keys_str.split(","))
        path_info = wdir / path

        abspath = os.path.abspath(path_info)
        if abspath in self.imports:
            if not select_keys and self.imports[abspath] is None:
                return  # allow specifying complete filepath multiple times
            self.check_loaded(abspath, item, select_keys)

        ctx = Context.load_from(tree, path_info, select_keys)
        self.merge_update(ctx, overwrite=overwrite)

        cp = ctx.imports[abspath]
        if abspath not in self.imports:
            self.imports[abspath] = cp
        elif cp:
            self.imports[abspath].extend(cp)

    def check_loaded(self, path, item, keys):
        if not keys and isinstance(self.imports[path], list):
            raise VarsAlreadyLoaded(
                f"cannot load '{item}' as it's partially loaded already"
            )
        elif keys and self.imports[path] is None:
            raise VarsAlreadyLoaded(
                f"cannot partially load '{item}' as it's already loaded."
            )
        elif keys and isinstance(self.imports[path], list):
            if not set(keys).isdisjoint(set(self.imports[path])):
                raise VarsAlreadyLoaded(
                    f"cannot load '{item}' as it's partially loaded already"
                )

    def load_from_vars(
        self,
        tree,
        vars_: List,
        wdir: PathInfo,
        stage_name: str = None,
        default: str = None,
    ):
        if default:
            to_import = wdir / default
            if tree.exists(to_import):
                self.merge_from(tree, default, wdir)
            else:
                msg = "%s does not exist, it won't be used in parametrization"
                logger.trace(msg, to_import)  # type: ignore[attr-defined]

        stage_name = stage_name or ""
        for index, item in enumerate(vars_):
            assert isinstance(item, (str, dict))
            if isinstance(item, str):
                self.merge_from(tree, item, wdir)
            else:
                joiner = "." if stage_name else ""
                meta = Meta(source=f"{stage_name}{joiner}vars[{index}]")
                self.merge_update(Context(item, meta=meta))

    def __deepcopy__(self, _):
        new = Context(super().__deepcopy__(_))
        new.meta = deepcopy(self.meta)
        new.imports = deepcopy(self.imports)
        return new

    @classmethod
    def clone(cls, ctx: "Context") -> "Context":
        """Clones given context."""
        return deepcopy(ctx)

    @contextmanager
    def set_temporarily(self, to_set):
        non_existing = frozenset(to_set.keys() - self.keys())
        prev = {key: self[key] for key in to_set if key not in non_existing}
        to_set = CtxDict(to_set)
        self.update(to_set)

        try:
            yield
        finally:
            self.update(prev)
            for key in non_existing:
                self.data.pop(key, None)

    def resolve(self, src, unwrap=True, skip_interpolation_checks=False):
        """Recursively resolves interpolation and returns resolved data.

        Args:
            src: Data (str/list/dict etc.) to resolve
            unwrap: Unwrap CtxDict/CtxList/Value to it's original data if
                    inside `src`. Defaults to True.

        >>> c = Context({"three": 3})
        >>> c.resolve({"lst": [1, 2, "${three}"]})
        {'lst': [1, 2, 3]}
        """
        func = recurse(self.resolve_str)
        return func(src, unwrap, skip_interpolation_checks)

    def resolve_str(
        self, src: str, unwrap=True, skip_interpolation_checks=False
    ):
        """Resolves interpolated string to it's original value,
        or in case of multiple interpolations, a combined string.

        >>> c = Context({"enabled": True})
        >>> c.resolve_str("${enabled}")
        True
        >>> c.resolve_str("enabled? ${enabled}")
        'enabled? true'
        """
        matches = get_matches(src)
        if is_exact_string(src, matches):
            # replace "${enabled}", if `enabled` is a boolean, with it's actual
            # value rather than it's string counterparts.
            expr = get_expression(
                matches[0], skip_checks=skip_interpolation_checks
            )
            return self.select(expr, unwrap=unwrap)
        # but not "${num} days"
        return str_interpolate(
            src, matches, self, skip_checks=skip_interpolation_checks
        )


if __name__ == "__main__":
    import doctest

    doctest.testmod()
