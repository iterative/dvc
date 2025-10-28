from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Mapping, MutableMapping, MutableSequence, Sequence
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field, replace
from typing import Any, Optional, Union

from funcy import identity, lfilter, nullcontext, select

from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.parsing.interpolate import (
    get_expression,
    get_matches,
    is_exact_string,
    is_interpolated_string,
    normalize_key,
    recurse,
    str_interpolate,
    validate_value,
)

logger = logger.getChild(__name__)
SeqOrMap = Union[Sequence, Mapping]
DictStr = dict[str, Any]


class ContextError(DvcException):
    pass


class ReservedKeyError(ContextError):
    def __init__(self, keys, path=None):
        from dvc.utils.humanize import join

        self.keys = keys
        self.path = path

        n = "key" + ("s" if len(keys) > 1 else "")
        msg = f"attempted to modify reserved {n} {join(keys)}"
        if path:
            msg += f" in '{path}'"
        super().__init__(msg)


class MergeError(ContextError):
    def __init__(self, key, new, into):
        self.key = key
        to_node = into[key]
        if not isinstance(to_node, Node) or not isinstance(new, Node):
            super().__init__(f"cannot merge '{key}' as it already exists in {into}")
            return

        assert isinstance(to_node, Node)
        assert isinstance(new, Node)
        preexisting = to_node.meta.source
        new_src = new.meta.source
        path = new.meta.path()
        super().__init__(
            f"cannot redefine '{path}' from '{new_src}'"
            f" as it already exists in '{preexisting}'"
        )


class ParamsLoadError(ContextError):
    pass


class KeyNotInContext(ContextError, KeyError):  # noqa: N818
    def __init__(self, key: str) -> None:
        self.key: str = key
        super().__init__(f"Could not find '{key}'")

    def __str__(self):
        return self.msg


class VarsAlreadyLoaded(ContextError):  # noqa: N818
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


def is_params_interpolation(value: Any) -> bool:
    """Check if value is an interpolated string using ${PARAMS_NAMESPACE.*} syntax."""
    if not isinstance(value, str):
        return False

    if not is_interpolated_string(value):
        return False

    # Import here to avoid circular import
    from dvc.parsing import PARAMS_NAMESPACE

    matches = get_matches(value)
    prefix = f"{PARAMS_NAMESPACE}."
    for match in matches:
        inner = match["inner"]
        if inner.startswith(prefix):
            return True
    return False


@dataclass
class Meta:
    source: Optional[str] = None
    dpaths: list[str] = field(default_factory=list)
    local: bool = True

    @staticmethod
    def update_path(meta: "Meta", path: Union[str, int]):
        dpaths = [*meta.dpaths, str(path)]
        return replace(meta, dpaths=dpaths)

    def __str__(self):
        string = self.source or "<local>"
        string += ":" + self.path()
        return string

    def path(self):
        return ".".join(self.dpaths)


def _default_meta() -> Meta:
    return Meta()


class Node:
    meta: Meta

    def get_sources(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def value(self):
        pass


@dataclass
class Value(Node):
    _value: Any
    meta: Meta = field(compare=False, default_factory=_default_meta, repr=False)

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


class Container(Node, ABC):  # noqa: PLW1641
    meta: Meta
    data: Union[list, dict]
    _key_transform = staticmethod(identity)

    def __init__(self, meta=None) -> None:
        self.meta = meta or _default_meta()

    def _convert(self, key, value):
        meta = Meta.update_path(self.meta, key)
        return self._convert_with_meta(value, meta)

    @staticmethod
    def _convert_with_meta(value, meta: Optional[Meta] = None):
        if value is None or isinstance(value, PRIMITIVES):
            assert meta
            return Value(value, meta=meta)
        if isinstance(value, Node):
            return value
        if isinstance(value, (list, dict)):
            assert meta
            if isinstance(value, dict):
                return CtxDict(value, meta=meta)
            return CtxList(value, meta=meta)
        msg = f"Unsupported value of type '{type(value).__name__}' in '{meta}'"
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
            raise ValueError(f"Could not find '{index}' in {self.data}") from exc

        if not rems:
            return d

        rem = rems[0]
        if not isinstance(d, Container):
            raise ValueError(  # noqa: TRY004
                f"{index} is a primitive value, cannot get '{rem}'"
            )
        return d.select(rem)

    def get_sources(self):
        return {}


class CtxList(Container, MutableSequence):
    _key_transform = staticmethod(int)

    def __init__(self, values: Sequence, meta: Optional[Meta] = None):
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
        new.data = self.data[:]  # Short-circuiting __setitem__
        return new


class CtxDict(Container, MutableMapping):
    def __init__(
        self,
        mapping: Optional[Mapping] = None,
        meta: Optional[Meta] = None,
        **kwargs,
    ):
        super().__init__(meta=meta)

        self.data: dict = {}
        if mapping:
            self.update(mapping)
        self.update(kwargs)

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            # limitation for the interpolation
            # ignore other kinds of keys
            return None
        return super().__setitem__(key, value)

    def merge_update(self, other, overwrite=False):
        _merge(self, other, overwrite=overwrite)

    @property
    def value(self):
        return {key: node.value for key, node in self.items()}

    def __deepcopy__(self, _):
        new = CtxDict()
        for k, v in self.items():
            new.data[k] = (
                deepcopy(v) if isinstance(v, Container) else v
            )  # short-circuiting __setitem__
        return new


class Context(CtxDict):
    def __init__(self, *args, **kwargs):
        """
        Top level mutable dict, with some helpers to create context and track
        """
        super().__init__(*args, **kwargs)
        self._track = False
        self._tracked_data: dict[str, dict] = defaultdict(dict)
        self.imports = {}
        self._reserved_keys = {}
        self._params_context: Optional[CtxDict] = None
        self._params_sources: dict[str, set[str]] = defaultdict(set)

    @contextmanager
    def track(self):
        self._track = True
        yield self._tracked_data

        self._track = False
        self._tracked_data = defaultdict(dict)

    def _track_data(self, node):
        if not self._track or not isinstance(node, Node):
            return

        assert isinstance(node, Node)
        if node.meta and node.meta.local:
            return

        for source, keys in node.get_sources().items():
            if not source:
                continue
            params_file = self._tracked_data[source]
            keys = [keys] if isinstance(keys, str) else keys
            params_file.update(dict.fromkeys(keys, node.value))

    def select(self, key: str, unwrap: bool = False):
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
        from dvc.parsing import PARAMS_NAMESPACE

        normalized = normalize_key(key)

        # Handle params namespace specially
        prefix = f"{PARAMS_NAMESPACE}."
        if normalized.startswith(prefix):
            if self._params_context is None:
                raise KeyNotInContext(key)
            params_key = normalized.split(".", 1)[1]
            try:
                node = self._params_context.select(params_key)
            except ValueError as exc:
                raise KeyNotInContext(key) from exc

            assert isinstance(node, Node)
            self._track_data(node)
            return node.value if unwrap else node

        try:
            node = super().select(normalized)
        except ValueError as exc:
            raise KeyNotInContext(key) from exc

        assert isinstance(node, Node)
        self._track_data(node)
        return node.value if unwrap else node

    @classmethod
    def load_from(
        cls, fs, path: str, select_keys: Optional[list[str]] = None
    ) -> "Context":
        from dvc.utils.serialize import load_path

        if not fs.exists(path):
            raise ParamsLoadError(f"'{path}' does not exist")
        if fs.isdir(path):
            raise ParamsLoadError(f"'{path}' is a directory")

        data = load_path(path, fs)
        if not isinstance(data, Mapping):
            typ = type(data).__name__
            raise ParamsLoadError(
                f"expected a dictionary, got '{typ}' in file '{path}'"
            )

        if select_keys:
            try:
                data = {key: data[key] for key in select_keys}
            except KeyError as exc:
                key, *_ = exc.args
                raise ParamsLoadError(f"could not find '{key}' in '{path}'") from exc

        meta = Meta(source=path, local=False)
        ctx = cls(data, meta=meta)
        ctx.imports[path] = select_keys
        return ctx

    def merge_update(self, other: "Context", overwrite=False):
        matches = select(lambda key: key in other, self._reserved_keys.keys())
        if matches:
            raise ReservedKeyError(matches)
        return super().merge_update(other, overwrite=overwrite)

    def merge_from(self, fs, item: str, wdir: str, overwrite=False):
        path, _, keys_str = item.partition(":")
        path = fs.normpath(fs.join(wdir, path))

        select_keys = lfilter(bool, keys_str.split(",")) if keys_str else None
        if path in self.imports:
            if not select_keys and self.imports[path] is None:
                return  # allow specifying complete filepath multiple times
            self.check_loaded(path, item, select_keys)

        ctx = Context.load_from(fs, path, select_keys)

        try:
            self.merge_update(ctx, overwrite=overwrite)
        except ReservedKeyError as exc:
            raise ReservedKeyError(exc.keys, item) from exc

        cp = ctx.imports[path]
        if path not in self.imports:
            self.imports[path] = cp
        elif cp:
            self.imports[path].extend(cp)

    def check_loaded(self, path, item, keys):
        imported = self.imports[path]
        if not keys and isinstance(imported, list):
            raise VarsAlreadyLoaded(
                f"cannot load '{item}' as it's partially loaded already"
            )
        if keys and imported is None:
            raise VarsAlreadyLoaded(
                f"cannot partially load '{item}' as it's already loaded."
            )
        if isinstance(imported, list) and set(keys) & set(imported):
            raise VarsAlreadyLoaded(
                f"cannot load '{item}' as it's partially loaded already"
            )

    def load_from_vars(
        self,
        fs,
        vars_: list,
        wdir: str,
        stage_name: Optional[str] = None,
        default: Optional[str] = None,
    ):
        if default:
            to_import = fs.join(wdir, default)
            if fs.exists(to_import):
                self.merge_from(fs, default, wdir)
            else:
                msg = "%s does not exist, it won't be used in parametrization"
                logger.trace(msg, to_import)

        stage_name = stage_name or ""
        for index, item in enumerate(vars_):
            assert isinstance(item, (str, dict))
            if isinstance(item, str):
                self.merge_from(fs, item, wdir)
            else:
                joiner = "." if stage_name else ""
                meta = Meta(source=f"{stage_name}{joiner}vars[{index}]")
                self.merge_update(Context(item, meta=meta))

    def _load_default_params_file(self, fs, wdir: str):
        """Load default params file if it exists."""
        from dvc.dependency.param import read_param_file
        from dvc.parsing import DEFAULT_PARAMS_FILE

        default_path = fs.normpath(fs.join(wdir, DEFAULT_PARAMS_FILE))
        if fs.exists(default_path):
            data = read_param_file(fs, default_path, key_paths=None, flatten=False)
            self._merge_params_data(data, default_path)

    def _load_params_from_dict(self, fs, item: dict, wdir: str):
        """Load params from a dict item (file: keys mapping)."""
        from dvc.dependency.param import read_param_file
        from dvc.parsing.interpolate import is_interpolated_string

        for file_path, keys in item.items():
            # Skip vars interpolations
            if is_interpolated_string(file_path) and not is_params_interpolation(
                file_path
            ):
                continue

            # Skip if keys is None - this means the params are only for dependency
            # tracking, not for ${param.*} interpolation. The keys will be resolved
            # from other loaded param files (global params or params.yaml).
            if keys is None:
                continue

            path = fs.normpath(fs.join(wdir, file_path))
            if not fs.exists(path):
                raise ParamsLoadError(f"'{path}' does not exist")

            # If keys is empty list, load all params from the file
            key_list = keys if keys else None
            data = read_param_file(fs, path, key_paths=key_list, flatten=False)
            self._merge_params_data(data, path)

    def load_params(self, fs, params_list: list, wdir: str):
        """Load params from files for ${PARAMS_NAMESPACE.*} interpolation.

        Args:
            fs: File system to use
            params_list: List of param files/dicts (same format as stage params)
            wdir: Working directory
        """
        if not params_list:
            return

        # Initialize params context if not already done
        if self._params_context is None:
            self._params_context = CtxDict(meta=Meta(source="params", local=False))

        # Load default params file if it exists (for ${param.*} interpolation)
        self._load_default_params_file(fs, wdir)

        # Process each item in params list
        # Note: String items are param KEYS for dependency tracking, not files
        # Only dict items specify files to load for ${param.*} interpolation
        for item in params_list:
            if isinstance(item, dict):
                self._load_params_from_dict(fs, item, wdir)

    def _merge_params_data(self, data: dict, source_path: str):
        """Merge params data into _params_context."""
        # Track which file each key came from for ambiguity detection
        assert self._params_context is not None
        assert self._params_sources is not None
        for key in data:
            top_level_key = key.split(".")[0] if "." in key else key
            if top_level_key in self._params_context:
                # Key already exists, track multiple sources
                self._params_sources[top_level_key].add(source_path)
            else:
                self._params_sources[top_level_key].add(source_path)

        # Merge data into params context using CtxDict structure
        meta = Meta(source=source_path, local=False)
        for k, v in data.items():
            # Convert value to Node structure
            item_meta = Meta.update_path(meta, k)
            self._params_context[k] = Container._convert_with_meta(
                v,
                item_meta,
            )

    def check_params_ambiguity(self, used_keys: set[str]):
        """Check if any used params keys are ambiguous (from multiple sources).

        Args:
            used_keys: Set of params keys that were actually used
                in interpolation

        Raises:
            ContextError: If any used key is ambiguous
        """
        for key in used_keys:
            # Extract top-level key from potentially nested key like "model.lr"
            top_level_key = key.split(".")[0]
            sources = self._params_sources.get(top_level_key, set())
            if len(sources) > 1:
                from dvc.utils.humanize import join as humanize_join

                raise ContextError(
                    f"Ambiguous param key '{key}' found in multiple files: "
                    f"{humanize_join(sorted(sources))}"
                )

    def __deepcopy__(self, _):
        new = Context(super().__deepcopy__(_))
        new.meta = deepcopy(self.meta)
        new.imports = deepcopy(self.imports)
        new._reserved_keys = deepcopy(self._reserved_keys)
        new._params_context = (
            deepcopy(self._params_context) if self._params_context else None
        )
        new._params_sources = deepcopy(self._params_sources)
        return new

    @classmethod
    def clone(cls, ctx: "Context") -> "Context":
        """Clones given context."""
        return deepcopy(ctx)

    @contextmanager
    def reserved(self, *keys: str):
        """Allow reserving some keys so that they cannot be overwritten.

        Ideally, we should delegate this to a separate container
        and support proper namespacing so that we could support `env` features.
        But for now, just `item` and `key`, this should do.
        """
        # using dict to make the error messages ordered
        new = dict.fromkeys([key for key in keys if key not in self._reserved_keys])
        self._reserved_keys.update(new)
        try:
            yield
        finally:
            for key in new:
                self._reserved_keys.pop(key)

    @contextmanager
    def set_temporarily(self, to_set: DictStr, reserve: bool = False):
        cm = self.reserved(*to_set) if reserve else nullcontext()

        non_existing = frozenset(to_set.keys() - self.keys())
        prev = {key: self[key] for key in to_set if key not in non_existing}
        temp = CtxDict(to_set)
        self.update(temp)

        try:
            with cm:
                yield
        finally:
            self.update(prev)
            for key in non_existing:
                self.data.pop(key, None)

    def resolve(
        self,
        src,
        unwrap=True,
        skip_interpolation_checks=False,
        key=None,
        config=None,
    ) -> Any:
        """Recursively resolves interpolation and returns resolved data.

        Args:
            src: Data (str/list/dict etc.) to resolve
            unwrap: Unwrap CtxDict/CtxList/Value to it's original data if
                inside `src`. Defaults to True.
            skip_interpolation_checks: Skip interpolation checks for error
                The callee is responsible to check for errors in advance.

        >>> c = Context({"three": 3})
        >>> c.resolve({"lst": [1, 2, "${three}"]})
        {'lst': [1, 2, 3]}
        """
        func = recurse(self.resolve_str)
        return func(src, unwrap, skip_interpolation_checks, key, config)

    def resolve_str(
        self,
        src: str,
        unwrap=True,
        skip_interpolation_checks=False,
        key=None,
        config=None,
    ) -> str:
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
            expr = get_expression(matches[0], skip_checks=skip_interpolation_checks)
            value = self.select(expr, unwrap=unwrap)
            validate_value(value, key)
            return value
        # but not "${num} days"
        return str_interpolate(
            src,
            matches,
            self,
            skip_checks=skip_interpolation_checks,
            key=key,
            config=config,
        )


if __name__ == "__main__":
    import doctest

    doctest.testmod()
