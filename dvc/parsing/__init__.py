import logging
import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from copy import deepcopy
from itertools import starmap
from typing import TYPE_CHECKING, Dict, List, Optional

from funcy import join, lfilter

from dvc.dependency.param import ParamsDependency
from dvc.exceptions import DvcException
from dvc.parsing.interpolate import ParseError
from dvc.path_info import PathInfo
from dvc.utils import relpath

from .context import (
    Context,
    ContextError,
    KeyNotInContext,
    MergeError,
    Meta,
    Node,
    SetError,
)

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)

STAGES_KWD = "stages"
VARS_KWD = "vars"
WDIR_KWD = "wdir"
DEFAULT_PARAMS_FILE = ParamsDependency.DEFAULT_PARAMS_FILE
PARAMS_KWD = "params"
FOREACH_KWD = "foreach"
DO_KWD = "do"
SET_KWD = "set"

DEFAULT_SENTINEL = object()

JOIN = "@"


class VarsAlreadyLoaded(DvcException):
    pass


class ResolveError(DvcException):
    pass


def _format_preamble(msg, path, spacing=" "):
    return f"failed to parse {msg} in '{path}':{spacing}"


def format_and_raise(exc, msg, path):
    spacing = (
        "\n"
        if isinstance(exc, (ParseError, MergeError, VarsAlreadyLoaded))
        else " "
    )
    message = _format_preamble(msg, path, spacing) + str(exc)

    # FIXME: cannot reraise because of how we log "cause" of the exception
    # the error message is verbose, hence need control over the spacing
    _reraise_err(ResolveError, message, from_exc=exc)


def _reraise_err(exc_cls, *args, from_exc=None):
    err = exc_cls(*args)
    if from_exc and logger.isEnabledFor(logging.DEBUG):
        raise err from from_exc
    raise err


class DataResolver:
    def __init__(self, repo: "Repo", wdir: PathInfo, d: dict):
        self.data: dict = d
        self.wdir = wdir
        self.repo = repo
        self.tree = self.repo.tree
        self.imported_files: Dict[str, Optional[List[str]]] = {}
        self.relpath = relpath(self.wdir / "dvc.yaml")

        to_import: PathInfo = wdir / DEFAULT_PARAMS_FILE
        if self.tree.exists(to_import):
            self.imported_files = {os.path.abspath(to_import): None}
            self.global_ctx = Context.load_from(self.tree, to_import)
        else:
            self.global_ctx = Context()
            logger.debug(
                "%s does not exist, it won't be used in parametrization",
                to_import,
            )

        vars_ = d.get(VARS_KWD, [])
        try:
            self.load_from_vars(
                self.global_ctx, vars_, wdir, skip_imports=self.imported_files
            )
        except (ContextError, VarsAlreadyLoaded) as exc:
            format_and_raise(exc, "'vars'", self.relpath)

        self.tracked_vars = {}

    @staticmethod
    def check_loaded(path, item, keys, skip_imports):
        if not keys and isinstance(skip_imports[path], list):
            raise VarsAlreadyLoaded(
                f"cannot load '{item}' as it's partially loaded already"
            )
        elif keys and skip_imports[path] is None:
            raise VarsAlreadyLoaded(
                f"cannot partially load '{item}' as it's already loaded."
            )
        elif keys and isinstance(skip_imports[path], list):
            if not set(keys).isdisjoint(set(skip_imports[path])):
                raise VarsAlreadyLoaded(
                    f"cannot load '{item}' as it's partially loaded already"
                )

    def load_from_vars(
        self,
        context: "Context",
        vars_: List,
        wdir: PathInfo,
        skip_imports: Dict[str, Optional[List[str]]],
        stage_name: str = None,
    ):
        stage_name = stage_name or ""
        for index, item in enumerate(vars_):
            assert isinstance(item, (str, dict))
            if isinstance(item, str):
                path, _, keys_str = item.partition(":")
                keys = lfilter(bool, keys_str.split(","))

                path_info = wdir / path
                path = os.path.abspath(path_info)

                if path in skip_imports:
                    if not keys and skip_imports[path] is None:
                        # allow specifying complete filepath multiple times
                        continue
                    self.check_loaded(path, item, keys, skip_imports)

                context.merge_from(self.tree, path_info, select_keys=keys)
                skip_imports[path] = keys if keys else None
            else:
                joiner = "." if stage_name else ""
                meta = Meta(source=f"{stage_name}{joiner}vars[{index}]")
                context.merge_update(Context(item, meta=meta))

    def _resolve_entry(self, name: str, definition):
        context = Context.clone(self.global_ctx)
        if FOREACH_KWD in definition:
            assert DO_KWD in definition
            self.set_context_from(
                context, definition.get(SET_KWD, {}), source=[name, "set"]
            )
            return self._foreach(
                context, name, definition[FOREACH_KWD], definition[DO_KWD]
            )

        try:
            return self._resolve_stage(context, name, definition)
        except ContextError as exc:
            format_and_raise(exc, f"stage '{name}'", self.relpath)

    def resolve(self):
        stages = self.data.get(STAGES_KWD, {})
        data = join(starmap(self._resolve_entry, stages.items())) or {}
        logger.trace("Resolved dvc.yaml:\n%s", data)
        return {STAGES_KWD: data}

    def _resolve_stage(self, context: Context, name: str, definition) -> dict:
        definition = deepcopy(definition)
        self.set_context_from(
            context, definition.pop(SET_KWD, {}), source=[name, "set"]
        )
        wdir = self._resolve_wdir(context, name, definition.get(WDIR_KWD))
        if self.wdir != wdir:
            logger.debug(
                "Stage %s has different wdir than dvc.yaml file", name
            )

        vars_ = definition.pop(VARS_KWD, [])
        # FIXME: Should `vars` be templatized?
        try:
            self.load_from_vars(
                context,
                vars_,
                wdir,
                skip_imports=deepcopy(self.imported_files),
                stage_name=name,
            )
        except VarsAlreadyLoaded as exc:
            format_and_raise(exc, f"'stages.{name}.vars'", self.relpath)

        logger.trace(  # type: ignore[attr-defined]
            "Context during resolution of stage %s:\n%s", name, context
        )

        with context.track():
            resolved = {}
            for key, value in definition.items():
                # NOTE: we do not pop "wdir", and resolve it again
                # this does not affect anything and is done to try to
                # track the source of `wdir` interpolation.
                # This works because of the side-effect that we do not
                # allow overwriting and/or str interpolating complex objects.
                # Fix if/when those assumptions are no longer valid.
                try:
                    resolved[key] = context.resolve(value)
                except (ParseError, KeyNotInContext) as exc:
                    format_and_raise(
                        exc, f"'stages.{name}.{key}'", self.relpath
                    )

        self.tracked_vars[name] = self._get_vars(context)
        return {name: resolved}

    def _get_vars(self, context: Context):
        tracked = defaultdict(dict)
        for path, vars_ in context.tracked.items():
            for var in vars_:
                tracked[path][var] = context.select(var, unwrap=True)
        return tracked

    def _resolve_wdir(
        self, context: Context, name: str, wdir: str = None
    ) -> PathInfo:
        if not wdir:
            return self.wdir

        try:
            wdir = str(context.resolve_str(wdir, unwrap=True))
        except (ContextError, ParseError) as exc:
            format_and_raise(exc, f"'stages.{name}.wdir'", self.relpath)
        return self.wdir / str(wdir)

    def _foreach(self, context: Context, name: str, foreach_data, do_data):
        iterable = self._resolve_foreach_data(context, name, foreach_data)
        args = (context, name, do_data, iterable)
        it = (
            range(len(iterable))
            if not isinstance(iterable, Mapping)
            else iterable
        )
        gen = (self._each_iter(*args, i) for i in it)
        return join(gen)

    def _each_iter(self, context: Context, name: str, do_data, iterable, key):
        value = iterable[key]
        c = Context.clone(context)
        suffix = c["item"] = value
        if isinstance(iterable, Mapping):
            suffix = c["key"] = key

        generated = f"{name}{JOIN}{suffix}"
        try:
            return self._resolve_stage(c, generated, do_data)
        except ContextError as exc:
            # pylint: disable=no-member
            if isinstance(exc, MergeError) and exc.key in self._inserted_keys(
                iterable
            ):
                raise ResolveError(
                    f"attempted to redefine '{exc.key}' in stage '{generated}'"
                    " generated through 'foreach'"
                )
            format_and_raise(
                exc, f"stage '{generated}' (gen. from '{name}')", self.relpath
            )

    def _resolve_foreach_data(
        self, context: "Context", name: str, foreach_data
    ):
        try:
            iterable = context.resolve(foreach_data, unwrap=False)
        except (ContextError, ParseError) as exc:
            format_and_raise(exc, f"'stages.{name}.foreach'", self.relpath)
        if isinstance(iterable, str) or not isinstance(
            iterable, (Sequence, Mapping)
        ):
            raise ResolveError(
                f"failed to resolve 'stages.{name}.foreach'"
                f" in '{self.relpath}': expected list/dictionary, got "
                + type(
                    iterable.value if isinstance(iterable, Node) else iterable
                ).__name__
            )

        warn_for = [k for k in self._inserted_keys(iterable) if k in context]
        if warn_for:
            logger.warning(
                "%s %s already specified, "
                "will be overwritten for stages generated from '%s'",
                " and ".join(warn_for),
                "is" if len(warn_for) == 1 else "are",
                name,
            )

        return iterable

    @staticmethod
    def _inserted_keys(iterable):
        keys = ["item"]
        if isinstance(iterable, Mapping):
            keys.append("key")
        return keys

    @classmethod
    def set_context_from(cls, context: Context, to_set, source=None):
        try:
            for key, value in to_set.items():
                src_set = [*(source or []), key]
                context.set(key, value, source=".".join(src_set))
        except SetError as exc:
            _reraise_err(ResolveError, str(exc), from_exc=exc)
