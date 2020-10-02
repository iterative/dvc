import logging
from collections.abc import Mapping
from itertools import starmap

from funcy import join, lmap, rpartial

from dvc.parsing.context import Context
from dvc.parsing.interpolate import find_match, get_value, str_interpolate

logger = logging.getLogger(__name__)

ARGS = "args"
IMPORTS = "imports"
CONSTANTS = "constants"
STAGES = "stages"
COLON = ":"
PARAMS = "params"


def _resolve_str(src, context, tracker=None):
    if isinstance(src, str):
        matches = find_match(src)
        num_matches = len(matches)
        if num_matches:
            to_replace = {}
            for match in matches:
                expr = match.group()
                expand_and_track, _, inner = match.groups()
                track = expand_and_track
                if expr not in to_replace:
                    to_replace[expr] = get_value(context, inner)
                if track and tracker is not None:
                    # TODO: does not work with files other than `params.yaml`
                    tracker.append(inner)

            # replace "${enabled}", if `enabled` is a boolean, with it's actual
            # value rather than it's string counterparts.
            if num_matches == 1 and src == matches[0].group(0):
                return list(to_replace.values())[0]
            # but not "${num} days"
            src = str_interpolate(src, to_replace)

    # regex already backtracks and avoids any `${` starting with
    # backslashes(`\--`). We just need to replace those by `${`.
    return src.replace(r"\${", "${") if isinstance(src, str) else src


def _resolve(src, context, tracker=None):
    # TODO: can we do this better?
    Seq = (list, tuple, set)

    apply_value = rpartial(_resolve, context, tracker)
    if isinstance(src, Mapping):
        return {key: apply_value(value) for key, value in src.items()}
    elif isinstance(src, Seq):
        return src.__class__(apply_value(value) for value in src)
    return _resolve_str(src, context, tracker)


class DataLoader:
    def __init__(self, d, context=None):
        """
        Args:
            d: data loaded from dvc.yaml or .dvc files
            context: Global context, eg: those passed from
                     `dvc repro --vars params.foo 3` (TODO)
        """
        self.d = d
        args = d.get(ARGS, {})

        self._imports = imports = args.get(IMPORTS, [])
        assert len(imports) < 2, "Do not allow multiple imports for now."

        contexts = lmap(self._load_imports, imports)
        constant_ctx = Context(args.get(CONSTANTS, {}))
        global_ctx = context or Context()

        # this applies to all of the stages in this "dvc.yaml"
        self.file_ctx = Context.merge(*contexts, constant_ctx, global_ctx)

    @staticmethod
    def _load_imports(spec: str):
        return Context.load_and_select(*spec.rsplit(COLON, maxsplit=1))

    def _resolve_entry(self, name, definition):
        # TODO: we might need to create a context out of stage's definition
        #  (think of `build-matrix` or `foreach`), empty dict for now.
        local_ctx = Context({})
        ctx = Context.merge(self.file_ctx, local_ctx)

        logger.debug("Context for stage %s: %s", name, ctx)

        tracker = []
        stage_d = _resolve(definition, ctx, tracker)

        logger.debug("Resolved data: %s", stage_d)

        if tracker:
            logger.debug("Tracking params: %s", tracker)

            stage_d[PARAMS] = stage_d.get(PARAMS, [])
            # TODO: resolve where that specific value came from
            # TODO: does not support `:<var>` based imports
            stage_d[PARAMS].extend(tracker)

        return {name: stage_d}

    def resolve(self):
        stages = self.d.get(STAGES, {})
        data = join(starmap(self._resolve_entry, stages.items()))
        return {**self.d, STAGES: data}
