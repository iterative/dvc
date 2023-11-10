from collections import Counter, defaultdict
from datetime import date, datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
)

from dvc.exceptions import InvalidArgumentError
from dvc.log import logger
from dvc.scm import Git
from dvc.ui import ui
from dvc.utils.flatten import flatten

from .collect import collect

if TYPE_CHECKING:
    from dvc.compare import TabularData
    from dvc.repo import Repo
    from dvc.ui.table import CellT

    from .serialize import ExpRange, ExpState

logger = logger.getChild(__name__)


def show(
    repo: "Repo",
    revs: Union[List[str], str, None] = None,
    all_branches: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    num: int = 1,
    hide_queued: bool = False,
    hide_failed: bool = False,
    sha_only: bool = False,
    **kwargs,
) -> List["ExpState"]:
    return collect(
        repo,
        revs=revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        num=num,
        hide_queued=hide_queued,
        hide_failed=hide_failed,
        sha_only=sha_only,
        **kwargs,
    )


def tabulate(
    baseline_states: Iterable["ExpState"],
    fill_value: Optional[str] = "-",
    error_value: str = "!",
    **kwargs,
) -> Tuple["TabularData", Dict[str, Iterable[str]]]:
    """Return table data for experiments.

    Returns:
        Tuple of (table_data, data_headers)
    """
    from funcy import lconcat
    from funcy.seqs import flatten as flatten_list

    from dvc.compare import TabularData

    data_names = _collect_names(baseline_states)
    metrics_names = data_names.metrics
    params_names = data_names.params
    deps_names = data_names.sorted_deps

    headers = [
        "Experiment",
        "rev",
        "typ",
        "Created",
        "parent",
        "State",
        "Executor",
    ]
    names = {**metrics_names, **params_names}
    counter = Counter(flatten_list([list(a.keys()) for a in names.values()]))
    counter.update(headers)
    metrics_headers = _normalize_headers(metrics_names, counter)
    params_headers = _normalize_headers(params_names, counter)

    all_headers = lconcat(headers, metrics_headers, params_headers, deps_names)
    td = TabularData(all_headers, fill_value=fill_value)
    td.extend(
        _build_rows(
            baseline_states,
            all_headers=all_headers,
            metrics_headers=metrics_headers,
            params_headers=params_headers,
            metrics_names=metrics_names,
            params_names=params_names,
            deps_names=deps_names,
            fill_value=fill_value,
            error_value=error_value,
            **kwargs,
        )
    )
    data_headers: Dict[str, Iterable[str]] = {
        "metrics": metrics_headers,
        "params": params_headers,
        "deps": deps_names,
    }
    return td, data_headers


def _build_rows(
    baseline_states: Iterable["ExpState"],
    *,
    all_headers: Iterable[str],
    fill_value: Optional[str],
    sort_by: Optional[str] = None,
    sort_order: Optional[Literal["asc", "desc"]] = None,
    **kwargs,
) -> Iterator[Tuple["CellT", ...]]:
    for baseline in baseline_states:
        row: Dict[str, "CellT"] = {k: fill_value for k in all_headers}
        row["Experiment"] = ""
        if baseline.name:
            row["rev"] = baseline.name
        elif Git.is_sha(baseline.rev):
            row["rev"] = baseline.rev[:7]
        else:
            row["rev"] = baseline.rev
        row["typ"] = "baseline"
        row["parent"] = ""
        if baseline.data:
            row["Created"] = format_time(
                baseline.data.timestamp, fill_value=fill_value, **kwargs
            )
            row.update(_data_cells(baseline, fill_value=fill_value, **kwargs))
        yield tuple(row.values())
        if baseline.experiments:
            if sort_by:
                metrics_names: Mapping[str, Iterable[str]] = kwargs.get(
                    "metrics_names", {}
                )
                params_names: Mapping[str, Iterable[str]] = kwargs.get(
                    "params_names", {}
                )
                sort_path, sort_name, sort_type = _sort_column(
                    sort_by, metrics_names, params_names
                )
                reverse = sort_order == "desc"
                experiments = _sort_exp(
                    baseline.experiments, sort_path, sort_name, sort_type, reverse
                )
            else:
                experiments = baseline.experiments
            for i, child in enumerate(experiments):
                yield from _exp_range_rows(
                    child,
                    all_headers=all_headers,
                    fill_value=fill_value,
                    is_base=i == len(baseline.experiments) - 1,
                    **kwargs,
                )


def _sort_column(  # noqa: C901
    sort_by: str,
    metric_names: Mapping[str, Iterable[str]],
    param_names: Mapping[str, Iterable[str]],
) -> Tuple[str, str, str]:
    sep = ":"
    parts = sort_by.split(sep)
    matches: Set[Tuple[str, str, str]] = set()

    for split_num in range(len(parts)):
        path = sep.join(parts[:split_num])
        sort_name = sep.join(parts[split_num:])
        if not path:  # handles ':metric_name' case
            sort_by = sort_name
        if path in metric_names and sort_name in metric_names[path]:
            matches.add((path, sort_name, "metrics"))
        if path in param_names and sort_name in param_names[path]:
            matches.add((path, sort_name, "params"))
    if not matches:
        for path in metric_names:
            if sort_by in metric_names[path]:
                matches.add((path, sort_by, "metrics"))
        for path in param_names:
            if sort_by in param_names[path]:
                matches.add((path, sort_by, "params"))

    if len(matches) == 1:
        return matches.pop()
    if len(matches) > 1:
        raise InvalidArgumentError(
            "Ambiguous sort column '{}' matched '{}'".format(
                sort_by,
                ", ".join([f"{path}:{name}" for path, name, _ in matches]),
            )
        )
    raise InvalidArgumentError(f"Unknown sort column '{sort_by}'")


def _sort_exp(
    experiments: Iterable["ExpRange"],
    sort_path: str,
    sort_name: str,
    typ: str,
    reverse: bool,
) -> List["ExpRange"]:
    from funcy import first

    def _sort(exp_range: "ExpRange"):
        exp = first(exp_range.revs)
        if not exp:
            return True
        data = exp.data.dumpd().get(typ, {}).get(sort_path, {}).get("data", {})
        val = flatten(data).get(sort_name)
        return val is None, val

    return sorted(experiments, key=_sort, reverse=reverse)


def _exp_range_rows(
    exp_range: "ExpRange",
    *,
    all_headers: Iterable[str],
    fill_value: Optional[str],
    is_base: bool = False,
    **kwargs,
) -> Iterator[Tuple["CellT", ...]]:
    from funcy import first

    if len(exp_range.revs) > 1:
        logger.debug("Returning tip commit for legacy checkpoint exp")
    exp = first(exp_range.revs)
    if exp:
        row: Dict[str, "CellT"] = {k: fill_value for k in all_headers}
        row["Experiment"] = exp.name or ""
        row["rev"] = exp.rev[:7] if Git.is_sha(exp.rev) else exp.rev
        row["typ"] = "branch_base" if is_base else "branch_commit"
        row["parent"] = ""
        if exp_range.executor:
            row["State"] = exp_range.executor.state.capitalize()
            if exp_range.executor.name:
                row["Executor"] = exp_range.executor.name.capitalize()
        if exp.data:
            row["Created"] = format_time(
                exp.data.timestamp, fill_value=fill_value, **kwargs
            )
            row.update(_data_cells(exp, fill_value=fill_value, **kwargs))
        yield tuple(row.values())


def _data_cells(
    exp: "ExpState",
    *,
    metrics_headers: Iterable[str],
    params_headers: Iterable[str],
    metrics_names: Mapping[str, Iterable[str]],
    params_names: Mapping[str, Iterable[str]],
    deps_names: Iterable[str],
    fill_value: Optional[str] = "-",
    error_value: str = "!",
    precision: Optional[int] = None,
    **kwargs,
) -> Iterator[Tuple[str, "CellT"]]:
    def _d_cells(
        d: Mapping[str, Any],
        names: Mapping[str, Iterable[str]],
        headers: Iterable[str],
    ) -> Iterator[Tuple[str, "CellT"]]:
        from dvc.compare import _format_field, with_value

        for fname, data in d.items():
            item = data.get("data", {})
            item = flatten(item) if isinstance(item, dict) else {fname: item}
            for name in names[fname]:
                value = with_value(
                    item.get(name),
                    error_value if data.get("error") else fill_value,
                )
                # wrap field data in ui.rich_text, otherwise rich may
                # interpret unescaped braces from list/dict types as rich
                # markup tags
                value = ui.rich_text(str(_format_field(value, precision)))
                if name in headers:
                    yield name, value
                else:
                    yield f"{fname}:{name}", value

    if not exp.data:
        return
    yield from _d_cells(exp.data.metrics, metrics_names, metrics_headers)
    yield from _d_cells(exp.data.params, params_names, params_headers)
    for name in deps_names:
        dep = exp.data.deps.get(name)
        if dep:
            yield name, dep.hash or fill_value


def format_time(
    timestamp: Optional[datetime],
    fill_value: Optional[str] = "-",
    iso: bool = False,
    **kwargs,
) -> Optional[str]:
    if not timestamp:
        return fill_value
    if iso:
        return timestamp.isoformat()
    if timestamp.date() == date.today():
        fmt = "%I:%M %p"
    else:
        fmt = "%b %d, %Y"
    return timestamp.strftime(fmt)


class _DataNames(NamedTuple):
    # NOTE: we use nested dict instead of set for metrics/params names to
    # preserve key ordering
    metrics: Dict[str, Dict[str, Any]]
    params: Dict[str, Dict[str, Any]]
    deps: Set[str]

    @property
    def sorted_deps(self):
        return sorted(self.deps)

    def update(self, other: "_DataNames"):
        def _update_d(
            d: Dict[str, Dict[str, Any]], other_d: Mapping[str, Mapping[str, Any]]
        ):
            for k, v in other_d.items():
                if k in d:
                    d[k].update(v)
                else:
                    d[k] = dict(v)

        _update_d(self.metrics, other.metrics)
        _update_d(self.params, other.params)
        self.deps.update(other.deps)


def _collect_names(exp_states: Iterable["ExpState"]) -> _DataNames:
    result = _DataNames(defaultdict(dict), defaultdict(dict), set())

    def _collect_d(result_d: Dict[str, Dict[str, Any]], data_d: Dict[str, Any]):
        for path, item in data_d.items():
            item = item.get("data", {})
            if isinstance(item, dict):
                item = flatten(item)
                result_d[path].update((key, None) for key in item)

    for exp in exp_states:
        if exp.data:
            _collect_d(result.metrics, exp.data.metrics)
            _collect_d(result.params, exp.data.params)
            result.deps.update(exp.data.deps)
        if exp.experiments:
            for child in exp.experiments:
                result.update(_collect_names(child.revs))

    return result


def _normalize_headers(
    names: Mapping[str, Mapping[str, Any]], count: Mapping[str, int]
) -> List[str]:
    return [
        name if count[name] == 1 else f"{path}:{name}"
        for path in names
        for name in names[path]
    ]
