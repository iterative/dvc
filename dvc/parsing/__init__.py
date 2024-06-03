import logging
import os
from collections.abc import Mapping, Sequence
from copy import deepcopy
from itertools import product
from typing import TYPE_CHECKING, Any, NamedTuple, Optional, Union

from funcy import collecting, first, isa, join, reraise

from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.parsing.interpolate import ParseError
from dvc.utils.objects import cached_property

from .context import (
    Context,
    ContextError,
    KeyNotInContext,
    MergeError,
    Node,
    VarsAlreadyLoaded,
)
from .interpolate import (
    check_expression,
    check_recursive_parse_errors,
    is_interpolated_string,
    recurse,
    to_str,
)

if TYPE_CHECKING:
    from typing import NoReturn

    from dvc.repo import Repo
    from dvc.types import DictStrAny

    from .context import SeqOrMap


logger = logger.getChild(__name__)

VARS_KWD = "vars"
WDIR_KWD = "wdir"

ARTIFACTS_KWD = "artifacts"
DATASETS_KWD = "datasets"
METRICS_KWD = "metrics"
PARAMS_KWD = "params"
PLOTS_KWD = "plots"
STAGES_KWD = "stages"

FOREACH_KWD = "foreach"
MATRIX_KWD = "matrix"
DO_KWD = "do"

DEFAULT_PARAMS_FILE = "params.yaml"

JOIN = "@"


class ResolveError(DvcException):
    pass


class EntryNotFound(DvcException):
    pass


def _format_preamble(msg: str, path: str, spacing: str = " ") -> str:
    return f"failed to parse {msg} in '{path}':{spacing}"


def format_and_raise(exc: Exception, msg: str, path: str) -> "NoReturn":
    spacing = (
        "\n" if isinstance(exc, (ParseError, MergeError, VarsAlreadyLoaded)) else " "
    )
    message = _format_preamble(msg, path, spacing) + str(exc)

    # FIXME: cannot reraise because of how we log "cause" of the exception
    # the error message is verbose, hence need control over the spacing
    _reraise_err(ResolveError, message, from_exc=exc)


def _reraise_err(
    exc_cls: type[Exception], *args, from_exc: Optional[Exception] = None
) -> "NoReturn":
    err = exc_cls(*args)
    if from_exc and logger.isEnabledFor(logging.DEBUG):
        raise err from from_exc
    raise err


def check_syntax_errors(
    definition: "DictStrAny", name: str, path: str, where: str = "stages"
):
    for key, d in definition.items():
        try:
            check_recursive_parse_errors(d)
        except ParseError as exc:
            format_and_raise(exc, f"'{where}.{name}.{key}'", path)


def is_map_or_seq(data: Any) -> bool:
    _is_map_or_seq = isa(Mapping, Sequence)
    return not isinstance(data, str) and _is_map_or_seq(data)


def split_group_name(name: str) -> tuple[str, Optional[str]]:
    group, *keys = name.rsplit(JOIN, maxsplit=1)
    return group, first(keys)


def check_interpolations(data: "DictStrAny", where: str, path: str):
    def func(s: "DictStrAny") -> None:
        if is_interpolated_string(s):
            raise ResolveError(
                _format_preamble(f"'{where}'", path) + "interpolating is not allowed"
            )

    return recurse(func)(data)


Definition = Union["ForeachDefinition", "EntryDefinition", "MatrixDefinition"]


def make_definition(
    resolver: "DataResolver", name: str, definition: "DictStrAny", **kwargs
) -> Definition:
    args = resolver, resolver.context, name, definition
    if MATRIX_KWD in definition:
        return MatrixDefinition(*args, **kwargs)
    if FOREACH_KWD in definition:
        return ForeachDefinition(*args, **kwargs)
    return EntryDefinition(*args, **kwargs)


class DataResolver:
    def __init__(self, repo: "Repo", wdir: str, d: dict):
        self.fs = fs = repo.fs
        self.parsing_config = repo.config.get("parsing", {})

        if os.path.isabs(wdir):
            wdir = fs.relpath(wdir)
            wdir = "" if wdir == os.curdir else wdir

        self.wdir = wdir
        self.relpath = fs.normpath(fs.join(self.wdir, "dvc.yaml"))

        vars_ = d.get(VARS_KWD, [])
        check_interpolations(vars_, VARS_KWD, self.relpath)
        self.context: Context = Context()

        try:
            args = fs, vars_, wdir  # load from `vars` section
            self.context.load_from_vars(*args, default=DEFAULT_PARAMS_FILE)
        except ContextError as exc:
            format_and_raise(exc, "'vars'", self.relpath)

        # we use `tracked_vars` to keep a dictionary of used variables
        # by the interpolated entries.
        self.tracked_vars: dict[str, Mapping] = {}

        stages_data = d.get(STAGES_KWD, {})
        # we wrap the definitions into:
        # ForeachDefinition, MatrixDefinition, and EntryDefinition
        # that helps us to optimize, cache and selectively load each one of
        # them as we need, and simplify all of this DSL/parsing logic.
        self.definitions: dict[str, Definition] = {
            name: make_definition(self, name, definition)
            for name, definition in stages_data.items()
        }

        self.artifacts = [
            ArtifactDefinition(self, self.context, name, definition, ARTIFACTS_KWD)
            for name, definition in d.get(ARTIFACTS_KWD, {}).items()
        ]
        self.datasets = [
            TopDefinition(self, self.context, str(i), definition, DATASETS_KWD)
            for i, definition in enumerate(d.get(DATASETS_KWD, []))
        ]
        self.metrics = [
            TopDefinition(self, self.context, str(i), definition, METRICS_KWD)
            for i, definition in enumerate(d.get(METRICS_KWD, []))
        ]
        self.params = [
            TopDefinition(self, self.context, str(i), definition, PARAMS_KWD)
            for i, definition in enumerate(d.get(PARAMS_KWD, []))
        ]
        self.plots = [
            TopDefinition(self, self.context, str(i), definition, PLOTS_KWD)
            for i, definition in enumerate(d.get(PLOTS_KWD, []))
        ]

    def resolve_one(self, name: str):
        group, key = split_group_name(name)

        if not self._has_group_and_key(group, key):
            raise EntryNotFound(f"Could not find '{name}'")

        # all of the checks for `key` not being None for
        # `ForeachDefinition`/`MatrixDefinition`
        # and/or `group` not existing in the `interim`, etc. should be
        # handled by the `self.has_key()` above.
        definition = self.definitions[group]
        if isinstance(definition, EntryDefinition):
            return definition.resolve()

        assert key
        return definition.resolve_one(key)

    def resolve(self):
        """Used for testing purposes, otherwise use resolve_one()."""
        data = join(map(self.resolve_one, self.get_keys()))
        logger.trace("Resolved dvc.yaml:\n%s", data)
        return {STAGES_KWD: data}

    # Top-level sections are eagerly evaluated, whereas stages are lazily evaluated,
    # one-by-one.

    def resolve_artifacts(self) -> dict[str, Optional[dict[str, Any]]]:
        d: dict[str, Optional[dict[str, Any]]] = {}
        for item in self.artifacts:
            d.update(item.resolve())
        return d

    def resolve_datasets(self) -> list[dict[str, Any]]:
        return [item.resolve() for item in self.datasets]

    def resolve_metrics(self) -> list[str]:
        return [item.resolve() for item in self.metrics]

    def resolve_params(self) -> list[str]:
        return [item.resolve() for item in self.params]

    def resolve_plots(self) -> list[Any]:
        return [item.resolve() for item in self.plots]

    def has_key(self, key: str):
        return self._has_group_and_key(*split_group_name(key))

    def _has_group_and_key(self, group: str, key: Optional[str] = None):
        try:
            definition = self.definitions[group]
        except KeyError:
            return False

        if not isinstance(definition, (ForeachDefinition, MatrixDefinition)):
            return key is None
        return key is not None and definition.has_member(key)

    @collecting
    def get_keys(self):
        for name, definition in self.definitions.items():
            if isinstance(definition, (ForeachDefinition, MatrixDefinition)):
                yield from definition.get_generated_names()
                continue
            yield name

    def track_vars(self, name: str, vars_) -> None:
        self.tracked_vars[name] = vars_


class EntryDefinition:
    def __init__(
        self,
        resolver: DataResolver,
        context: Context,
        name: str,
        definition: "DictStrAny",
        where: str = STAGES_KWD,
    ):
        self.resolver = resolver
        self.wdir = self.resolver.wdir
        self.relpath = self.resolver.relpath
        self.context = context
        self.name = name
        self.definition = definition
        self.where = where

    def _resolve_wdir(
        self, context: Context, name: str, wdir: Optional[str] = None
    ) -> str:
        if not wdir:
            return self.wdir

        try:
            wdir = to_str(context.resolve_str(wdir))
        except (ContextError, ParseError) as exc:
            format_and_raise(exc, f"'{self.where}.{name}.wdir'", self.relpath)
        return self.resolver.fs.join(self.wdir, wdir)

    def resolve(self, **kwargs):
        try:
            return self.resolve_stage(**kwargs)
        except ContextError as exc:
            format_and_raise(exc, f"stage '{self.name}'", self.relpath)

    def resolve_stage(self, skip_checks: bool = False) -> "DictStrAny":
        context = self.context
        name = self.name
        if not skip_checks:
            # we can check for syntax errors as we go for interpolated entries,
            # but for foreach and matrix generated ones, once is enough, which it does
            # that itself. See `ForeachDefinition.template`
            # and `MatrixDefinition.template`.
            check_syntax_errors(self.definition, name, self.relpath)

        # we need to pop vars from generated/evaluated data
        definition = deepcopy(self.definition)

        wdir = self._resolve_wdir(context, name, definition.get(WDIR_KWD))
        vars_ = definition.pop(VARS_KWD, [])
        # FIXME: Should `vars` be templatized?
        check_interpolations(vars_, f"{self.where}.{name}.vars", self.relpath)
        if vars_:
            # Optimization: Lookahead if it has any vars, if it does not, we
            # don't need to clone them.
            context = Context.clone(context)

        try:
            fs = self.resolver.fs
            context.load_from_vars(fs, vars_, wdir, stage_name=name)
        except VarsAlreadyLoaded as exc:
            format_and_raise(exc, f"'{self.where}.{name}.vars'", self.relpath)

        logger.trace("Context during resolution of stage %s:\n%s", name, context)

        with context.track() as tracked_data:
            # NOTE: we do not pop "wdir", and resolve it again
            # this does not affect anything and is done to try to
            # track the source of `wdir` interpolation.
            # This works because of the side-effect that we do not
            # allow overwriting and/or str interpolating complex objects.
            # Fix if/when those assumptions are no longer valid.
            resolved = {
                key: self._resolve(context, value, key, skip_checks)
                for key, value in definition.items()
            }

        self.resolver.track_vars(name, tracked_data)
        return {name: resolved}

    def _resolve(
        self, context: "Context", value: Any, key: str, skip_checks: bool
    ) -> "DictStrAny":
        try:
            return context.resolve(
                value,
                skip_interpolation_checks=skip_checks,
                key=key,
                config=self.resolver.parsing_config,
            )
        except (ParseError, KeyNotInContext) as exc:
            format_and_raise(exc, f"'{self.where}.{self.name}.{key}'", self.relpath)


class IterationPair(NamedTuple):
    key: str = "key"
    value: str = "item"


class ForeachDefinition:
    def __init__(
        self,
        resolver: DataResolver,
        context: Context,
        name: str,
        definition: "DictStrAny",
        where: str = STAGES_KWD,
    ):
        self.resolver = resolver
        self.relpath = self.resolver.relpath
        self.context = context
        self.name = name

        assert DO_KWD in definition
        assert MATRIX_KWD not in definition
        self.foreach_data = definition[FOREACH_KWD]
        self._template = definition[DO_KWD]

        self.pair = IterationPair()
        self.where = where

    @cached_property
    def template(self):
        # optimization: check for syntax errors only once for `foreach` stages
        check_syntax_errors(self._template, self.name, self.relpath)
        return self._template

    @cached_property
    def resolved_iterable(self):
        return self._resolve_foreach_data()

    def _resolve_foreach_data(self) -> "SeqOrMap":
        try:
            iterable = self.context.resolve(self.foreach_data, unwrap=False)
        except (ContextError, ParseError) as exc:
            format_and_raise(exc, f"'{self.where}.{self.name}.foreach'", self.relpath)

        # foreach data can be a resolved dictionary/list.
        self._check_is_map_or_seq(iterable)
        # foreach stages will have `item` and `key` added to the context
        # so, we better warn them if they have them already in the context
        # from the global vars. We could add them in `set_temporarily`, but
        # that'd make it display for each iteration.
        self._warn_if_overwriting(self._inserted_keys(iterable))
        return iterable

    def _check_is_map_or_seq(self, iterable):
        if not is_map_or_seq(iterable):
            node = iterable.value if isinstance(iterable, Node) else iterable
            typ = type(node).__name__
            raise ResolveError(
                f"failed to resolve '{self.where}.{self.name}.foreach'"
                f" in '{self.relpath}': expected list/dictionary, got " + typ
            )

    def _warn_if_overwriting(self, keys: list[str]):
        warn_for = [k for k in keys if k in self.context]
        if warn_for:
            linking_verb = "is" if len(warn_for) == 1 else "are"
            logger.warning(
                (
                    "%s %s already specified, "
                    "will be overwritten for stages generated from '%s'"
                ),
                " and ".join(warn_for),
                linking_verb,
                self.name,
            )

    def _inserted_keys(self, iterable) -> list[str]:
        keys = [self.pair.value]
        if isinstance(iterable, Mapping):
            keys.append(self.pair.key)
        return keys

    @cached_property
    def normalized_iterable(self):
        """Convert sequence to Mapping with keys normalized."""
        iterable = self.resolved_iterable
        if isinstance(iterable, Mapping):
            return {to_str(k): v for k, v in iterable.items()}

        assert isinstance(iterable, Sequence)
        if any(map(is_map_or_seq, iterable)):
            # if the list contains composite data, index are the keys
            return {to_str(idx): value for idx, value in enumerate(iterable)}

        # for simple lists, eg: ["foo", "bar"],  contents are the key itself
        return {to_str(value): value for value in iterable}

    def has_member(self, key: str) -> bool:
        return key in self.normalized_iterable

    def get_generated_names(self):
        return list(map(self._generate_name, self.normalized_iterable))

    def _generate_name(self, key: str) -> str:
        return f"{self.name}{JOIN}{key}"

    def resolve_all(self) -> "DictStrAny":
        return join(map(self.resolve_one, self.normalized_iterable))

    def resolve_one(self, key: str) -> "DictStrAny":
        return self._each_iter(key)

    def _each_iter(self, key: str) -> "DictStrAny":
        err_message = f"Could not find '{key}' in foreach group '{self.name}'"
        with reraise(KeyError, EntryNotFound(err_message)):
            value = self.normalized_iterable[key]

        # NOTE: we need to use resolved iterable/foreach-data,
        # not the normalized ones to figure out whether to make item/key
        # available
        inserted = self._inserted_keys(self.resolved_iterable)
        temp_dict = {self.pair.value: value}
        key_str = self.pair.key
        if key_str in inserted:
            temp_dict[key_str] = key

        with self.context.set_temporarily(temp_dict, reserve=True):
            # optimization: item and key can be removed on __exit__() as they
            # are top-level values, and are not merged recursively.
            # This helps us avoid cloning context, which is slower
            # (increasing the size of the context might increase
            # the no. of items to be generated which means more cloning,
            # i.e. quadratic complexity).
            generated = self._generate_name(key)
            entry = EntryDefinition(
                self.resolver, self.context, generated, self.template
            )
            try:
                # optimization: skip checking for syntax errors on each foreach
                # generated stages. We do it once when accessing template.
                return entry.resolve_stage(skip_checks=True)
            except ContextError as exc:
                format_and_raise(exc, f"stage '{generated}'", self.relpath)


class MatrixDefinition:
    def __init__(
        self,
        resolver: DataResolver,
        context: Context,
        name: str,
        definition: "DictStrAny",
        where: str = STAGES_KWD,
    ):
        self.resolver = resolver
        self.relpath = self.resolver.relpath
        self.context = context
        self.name = name

        assert MATRIX_KWD in definition
        assert DO_KWD not in definition
        assert FOREACH_KWD not in definition

        self._template = definition.copy()
        self.matrix_data = self._template.pop(MATRIX_KWD)

        self.pair = IterationPair()
        self.where = where

    @cached_property
    def template(self) -> "DictStrAny":
        # optimization: check for syntax errors only once for `matrix` stages
        check_syntax_errors(self._template, self.name, self.relpath)
        return self._template

    @cached_property
    def resolved_iterable(self) -> dict[str, list]:
        return self._resolve_matrix_data()

    def _resolve_matrix_data(self) -> dict[str, list]:
        try:
            iterable = self.context.resolve(self.matrix_data, unwrap=False)
        except (ContextError, ParseError) as exc:
            format_and_raise(exc, f"'{self.where}.{self.name}.matrix'", self.relpath)

        # Matrix entries will have `key` and `item` added to the context.
        # Warn users if these are already in the context from the global vars.
        self._warn_if_overwriting([self.pair.key, self.pair.value])
        return iterable

    def _warn_if_overwriting(self, keys: list[str]):
        warn_for = [k for k in keys if k in self.context]
        if warn_for:
            linking_verb = "is" if len(warn_for) == 1 else "are"
            logger.warning(
                (
                    "%s %s already specified, "
                    "will be overwritten for stages generated from '%s'"
                ),
                " and ".join(warn_for),
                linking_verb,
                self.name,
            )

    @cached_property
    def normalized_iterable(self) -> dict[str, "DictStrAny"]:
        """Convert sequence to Mapping with keys normalized."""
        iterable = self.resolved_iterable
        assert isinstance(iterable, Mapping)

        ret: dict[str, DictStrAny] = {}
        matrix = {key: enumerate(v) for key, v in iterable.items()}
        for combination in product(*matrix.values()):
            d: DictStrAny = {}
            fragments: list[str] = []
            for k, (i, v) in zip(matrix.keys(), combination):
                d[k] = v
                fragments.append(f"{k}{i}" if is_map_or_seq(v) else to_str(v))

            key = "-".join(fragments)
            ret[key] = d
        return ret

    def has_member(self, key: str) -> bool:
        return key in self.normalized_iterable

    def get_generated_names(self) -> list[str]:
        return list(map(self._generate_name, self.normalized_iterable))

    def _generate_name(self, key: str) -> str:
        return f"{self.name}{JOIN}{key}"

    def resolve_all(self) -> "DictStrAny":
        return join(map(self.resolve_one, self.normalized_iterable))

    def resolve_one(self, key: str) -> "DictStrAny":
        return self._each_iter(key)

    def _each_iter(self, key: str) -> "DictStrAny":
        err_message = f"Could not find '{key}' in matrix group '{self.name}'"
        with reraise(KeyError, EntryNotFound(err_message)):
            value = self.normalized_iterable[key]

        temp_dict = {self.pair.key: key, self.pair.value: value}
        with self.context.set_temporarily(temp_dict, reserve=True):
            # optimization: item and key can be removed on __exit__() as they
            # are top-level values, and are not merged recursively.
            # This helps us avoid cloning context, which is slower
            # (increasing the size of the context might increase
            # the no. of items to be generated which means more cloning,
            # i.e. quadratic complexity).
            generated = self._generate_name(key)
            entry = EntryDefinition(
                self.resolver, self.context, generated, self.template
            )
            try:
                # optimization: skip checking for syntax errors on each matrix
                # generated stages. We do it once when accessing template.
                return entry.resolve_stage(skip_checks=True)
            except ContextError as exc:
                format_and_raise(exc, f"stage '{generated}'", self.relpath)


class TopDefinition:
    def __init__(
        self,
        resolver: DataResolver,
        context: Context,
        name: str,
        definition: "Any",
        where: str,
    ):
        self.resolver = resolver
        self.context = context
        self.name = name
        self.definition = definition
        self.where = where
        self.relpath = self.resolver.relpath

    def resolve(self):
        try:
            check_recursive_parse_errors(self.definition)
            return self.context.resolve(self.definition)
        except (ParseError, ContextError) as exc:
            format_and_raise(exc, f"'{self.where}.{self.name}'", self.relpath)


class ArtifactDefinition(TopDefinition):
    def resolve(self) -> dict[str, Optional[dict[str, Any]]]:
        try:
            check_expression(self.name)
            name = self.context.resolve(self.name)
            if not isinstance(name, str):
                typ = type(name).__name__
                raise ResolveError(
                    f"failed to resolve '{self.where}.{self.name}'"
                    f" in '{self.relpath}': expected str, got " + typ
                )
        except (ParseError, ContextError) as exc:
            format_and_raise(exc, f"'{self.where}.{self.name}'", self.relpath)
        return {name: super().resolve()}
