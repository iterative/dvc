import argparse
import logging
from collections import Counter, OrderedDict, defaultdict
from datetime import date, datetime
from fnmatch import fnmatch
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Iterable, Optional

from funcy import lmap

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.command.metrics import DEFAULT_PRECISION
from dvc.exceptions import DvcException, InvalidArgumentError
from dvc.ui import ui
from dvc.utils.flatten import flatten
from dvc.utils.serialize import encode_exception

if TYPE_CHECKING:
    from dvc.compare import TabularData
    from dvc.ui import RichText

FILL_VALUE = "-"
FILL_VALUE_ERRORED = "!"


logger = logging.getLogger(__name__)


def _filter_name(names, label, filter_strs):
    ret = defaultdict(dict)
    path_filters = defaultdict(list)

    for filter_s in filter_strs:
        path, _, name = filter_s.rpartition(":")
        path_filters[path].append(name)

    for path, filters in path_filters.items():
        if path:
            match_paths = [path]
        else:
            match_paths = names.keys()
        for match_path in match_paths:
            for f in filters:
                matches = [
                    name for name in names[match_path] if fnmatch(name, f)
                ]
                if not matches:
                    raise InvalidArgumentError(
                        f"'{f}' does not match any known {label}"
                    )
                ret[match_path].update({match: None for match in matches})

    return ret


def _filter_names(
    names: Dict[str, Dict[str, None]],
    label: str,
    include: Optional[Iterable],
    exclude: Optional[Iterable],
):
    if include and exclude:
        intersection = set(include) & set(exclude)
        if intersection:
            values = ", ".join(intersection)
            raise InvalidArgumentError(
                f"'{values}' specified in both --include-{label} and"
                f" --exclude-{label}"
            )

    if include:
        ret = _filter_name(names, label, include)
    else:
        ret = names

    if exclude:
        to_remove = _filter_name(names, label, exclude)
        for path in to_remove:
            if path in ret:
                for key in to_remove[path]:
                    if key in ret[path]:
                        del ret[path][key]

    return ret


def _update_names(names, items):
    for name, item in items:
        item = item.get("data", {})
        if isinstance(item, dict):
            item = flatten(item)
            names[name].update({key: None for key in item})


def _collect_names(all_experiments, **kwargs):
    metric_names = defaultdict(dict)
    param_names = defaultdict(dict)

    for _, experiments in all_experiments.items():
        for exp_data in experiments.values():
            exp = exp_data.get("data", {})
            _update_names(metric_names, exp.get("metrics", {}).items())
            _update_names(param_names, exp.get("params", {}).items())
    metric_names = _filter_names(
        metric_names,
        "metrics",
        kwargs.get("include_metrics"),
        kwargs.get("exclude_metrics"),
    )
    param_names = _filter_names(
        param_names,
        "params",
        kwargs.get("include_params"),
        kwargs.get("exclude_params"),
    )

    return metric_names, param_names


experiment_types = {
    "checkpoint_tip": "│ ╓",
    "checkpoint_commit": "│ ╟",
    "checkpoint_base": "├─╨",
    "branch_commit": "├──",
    "branch_base": "└──",
    "baseline": "",
}


def _collect_rows(
    base_rev,
    experiments,
    metric_names,
    param_names,
    precision=DEFAULT_PRECISION,
    sort_by=None,
    sort_order=None,
    fill_value=FILL_VALUE,
    iso=False,
):
    from scmrepo.git import Git

    if sort_by:
        sort_path, sort_name, sort_type = _sort_column(
            sort_by, metric_names, param_names
        )
        reverse = sort_order == "desc"
        experiments = _sort_exp(
            experiments, sort_path, sort_name, sort_type, reverse
        )

    new_checkpoint = True
    for i, (rev, results) in enumerate(experiments.items()):
        exp = results.get("data", {})
        if exp.get("running"):
            state = "Running"
        elif exp.get("queued"):
            state = "Queued"
        else:
            state = fill_value
        executor = exp.get("executor", fill_value)
        is_baseline = rev == "baseline"

        if is_baseline:
            name_rev = base_rev[:7] if Git.is_sha(base_rev) else base_rev
        else:
            name_rev = rev[:7]

        exp_name = exp.get("name", "")
        tip = exp.get("checkpoint_tip")

        parent_rev = exp.get("checkpoint_parent", "")
        parent_exp = experiments.get(parent_rev, {}).get("data", {})
        parent_tip = parent_exp.get("checkpoint_tip")

        parent = ""
        if is_baseline:
            typ = "baseline"
        elif tip:
            if tip == parent_tip:
                typ = (
                    "checkpoint_tip" if new_checkpoint else "checkpoint_commit"
                )
            elif parent_rev == base_rev:
                typ = "checkpoint_base"
            else:
                typ = "checkpoint_commit"
                parent = parent_rev[:7]
        elif i < len(experiments) - 1:
            typ = "branch_commit"
        else:
            typ = "branch_base"

        if not is_baseline:
            new_checkpoint = not (tip and tip == parent_tip)

        row = [
            exp_name,
            name_rev,
            typ,
            _format_time(exp.get("timestamp"), fill_value, iso),
            parent,
            state,
            executor,
        ]
        fill_value = FILL_VALUE_ERRORED if results.get("error") else fill_value
        _extend_row(
            row,
            metric_names,
            exp.get("metrics", {}).items(),
            precision,
            fill_value=fill_value,
        )
        _extend_row(
            row,
            param_names,
            exp.get("params", {}).items(),
            precision,
            fill_value=fill_value,
        )

        yield row


def _sort_column(sort_by, metric_names, param_names):
    path, _, sort_name = sort_by.rpartition(":")
    matches = set()

    if path:
        if path in metric_names and sort_name in metric_names[path]:
            matches.add((path, sort_name, "metrics"))
        if path in param_names and sort_name in param_names[path]:
            matches.add((path, sort_name, "params"))
    else:
        for path in metric_names:
            if sort_name in metric_names[path]:
                matches.add((path, sort_name, "metrics"))
        for path in param_names:
            if sort_name in param_names[path]:
                matches.add((path, sort_name, "params"))

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


def _sort_exp(experiments, sort_path, sort_name, typ, reverse):
    def _sort(item):
        rev, exp = item
        exp_data = exp.get("data", {})
        tip = exp_data.get("checkpoint_tip")
        if tip and tip != rev:
            # Sort checkpoint experiments by tip commit
            return _sort((tip, experiments[tip]))
        data = exp_data.get(typ, {}).get(sort_path, {}).get("data", {})
        val = flatten(data).get(sort_name)
        return val is None, val

    ret = OrderedDict()
    if "baseline" in experiments:
        ret["baseline"] = experiments.pop("baseline")

    ret.update(sorted(experiments.items(), key=_sort, reverse=reverse))
    return ret


def _format_time(datetime_obj, fill_value=FILL_VALUE, iso=False):
    if datetime_obj is None:
        return fill_value

    if iso:
        return datetime_obj.isoformat()

    if datetime_obj.date() == date.today():
        fmt = "%I:%M %p"
    else:
        fmt = "%b %d, %Y"
    return datetime_obj.strftime(fmt)


def _extend_row(row, names, items, precision, fill_value=FILL_VALUE):
    from dvc.compare import _format_field, with_value

    if not items:
        row.extend(fill_value for keys in names.values() for _ in keys)
        return

    for fname, data in items:
        item = data.get("data", {})
        item = flatten(item) if isinstance(item, dict) else {fname: item}
        for name in names[fname]:
            value = with_value(
                item.get(name),
                FILL_VALUE_ERRORED if data.get("error", None) else fill_value,
            )
            # wrap field data in ui.rich_text, otherwise rich may
            # interpret unescaped braces from list/dict types as rich
            # markup tags
            row.append(ui.rich_text(str(_format_field(value, precision))))


def _parse_filter_list(param_list):
    ret = []
    for param_str in param_list:
        path, _, param_str = param_str.rpartition(":")
        if path:
            ret.extend(f"{path}:{param}" for param in param_str.split(","))
        else:
            ret.extend(param_str.split(","))
    return ret


def experiments_table(
    all_experiments,
    headers,
    metric_headers,
    metric_names,
    param_headers,
    param_names,
    sort_by=None,
    sort_order=None,
    precision=DEFAULT_PRECISION,
    fill_value=FILL_VALUE,
    iso=False,
) -> "TabularData":
    from funcy import lconcat

    from dvc.compare import TabularData

    td = TabularData(
        lconcat(headers, metric_headers, param_headers), fill_value=fill_value
    )
    for base_rev, experiments in all_experiments.items():
        rows = _collect_rows(
            base_rev,
            experiments,
            metric_names,
            param_names,
            sort_by=sort_by,
            sort_order=sort_order,
            precision=precision,
            fill_value=fill_value,
            iso=iso,
        )
        td.extend(rows)

    return td


def prepare_exp_id(kwargs) -> "RichText":
    exp_name = kwargs["Experiment"]
    rev = kwargs["rev"]
    typ = kwargs.get("typ", "baseline")

    if typ == "baseline" or not exp_name:
        text = ui.rich_text(exp_name or rev)
    else:
        text = ui.rich_text.assemble(rev, " [", (exp_name, "bold"), "]")

    parent = kwargs.get("parent")
    suff = f" ({parent})" if parent else ""
    text.append(suff)

    tree = experiment_types[typ]
    pref = f"{tree} " if tree else ""
    return ui.rich_text(pref) + text


def baseline_styler(typ):
    return {"style": "bold"} if typ == "baseline" else {"style": "default"}


def show_experiments(
    all_experiments,
    pager=True,
    no_timestamp=False,
    csv=False,
    markdown=False,
    html=False,
    **kwargs,
):
    from funcy.seqs import flatten as flatten_list

    include_metrics = _parse_filter_list(kwargs.pop("include_metrics", []))
    exclude_metrics = _parse_filter_list(kwargs.pop("exclude_metrics", []))
    include_params = _parse_filter_list(kwargs.pop("include_params", []))
    exclude_params = _parse_filter_list(kwargs.pop("exclude_params", []))

    metric_names, param_names = _collect_names(
        all_experiments,
        include_metrics=include_metrics,
        exclude_metrics=exclude_metrics,
        include_params=include_params,
        exclude_params=exclude_params,
    )

    headers = [
        "Experiment",
        "rev",
        "typ",
        "Created",
        "parent",
        "State",
        "Executor",
    ]

    names = {**metric_names, **param_names}
    counter = Counter(flatten_list([list(a.keys()) for a in names.values()]))
    counter.update(headers)
    metric_headers = _normalize_headers(metric_names, counter)
    param_headers = _normalize_headers(param_names, counter)

    td = experiments_table(
        all_experiments,
        headers,
        metric_headers,
        metric_names,
        param_headers,
        param_names,
        kwargs.get("sort_by"),
        kwargs.get("sort_order"),
        kwargs.get("precision"),
        kwargs.get("fill_value"),
        kwargs.get("iso"),
    )

    if no_timestamp or html:
        td.drop("Created")

    for col in ("State", "Executor"):
        if td.is_empty(col):
            td.drop(col)

    row_styles = lmap(baseline_styler, td.column("typ"))
    for n, row_style in enumerate(row_styles):
        if n % 2 != 0:
            row_style["style"] += " on grey23"

    if not csv:
        merge_headers = ["Experiment", "rev", "typ", "parent"]
        td.column("Experiment")[:] = map(
            prepare_exp_id, td.as_dict(merge_headers)
        )
        td.drop(*merge_headers[1:])

    headers = {"metrics": metric_headers, "params": param_headers}
    styles = {
        "Experiment": {"no_wrap": True, "header_style": "black on grey93"},
        "Created": {"header_style": "black on grey93"},
        "State": {"header_style": "black on grey93"},
        "Executor": {"header_style": "black on grey93"},
    }
    header_bg_colors = {"metrics": "cornsilk1", "params": "light_cyan1"}
    styles.update(
        {
            header: {
                "justify": "right" if typ == "metrics" else "left",
                "header_style": f"black on {header_bg_colors[typ]}",
                "collapse": idx != 0,
                "no_wrap": typ == "metrics",
            }
            for typ, hs in headers.items()
            for idx, header in enumerate(hs)
        }
    )

    if kwargs.get("only_changed", False) or html:
        td.drop_duplicates("cols", ignore_empty=False)

    html_args = {}
    if html:
        td.dropna("rows", how="all")
        td.column("Experiment")[:] = [
            # remove tree characters
            str(x).encode("ascii", "ignore").strip().decode()
            for x in td.column("Experiment")
        ]
        out = kwargs.get("out") or "dvc_plots"
        html_args["output_path"] = (Path.cwd() / out).resolve()
        html_args["color_by"] = kwargs.get("sort_by") or "Experiment"

    td.render(
        pager=pager,
        borders="horizontals",
        rich_table=True,
        header_styles=styles,
        row_styles=row_styles,
        csv=csv,
        markdown=markdown,
        html=html,
        **html_args,
    )

    if html and kwargs.get("open"):
        return ui.open_browser(Path(out) / "index.html")


def _normalize_headers(names, count):
    return [
        name if count[name] == 1 else f"{path}:{name}"
        for path in names
        for name in names[path]
    ]


def _format_json(item):
    if isinstance(item, (date, datetime)):
        return item.isoformat()
    return encode_exception(item)


def _raise_error_if_all_disabled(**kwargs):
    if not any(kwargs.values()):
        raise InvalidArgumentError(
            "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
            "or `--all-commits` needs to be set."
        )


class CmdExperimentsShow(CmdBase):
    def run(self):
        try:
            all_experiments = self.repo.experiments.show(
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
                sha_only=self.args.sha,
                num=self.args.num,
                param_deps=self.args.param_deps,
            )
        except DvcException:
            logger.exception("failed to show experiments")
            return 1

        if self.args.json:
            ui.write_json(all_experiments, default=_format_json)
        else:
            precision = (
                self.args.precision or None
                if self.args.csv
                else DEFAULT_PRECISION
            )
            fill_value = "" if self.args.csv else FILL_VALUE
            iso = True if self.args.csv else False

            show_experiments(
                all_experiments,
                include_metrics=self.args.include_metrics,
                exclude_metrics=self.args.exclude_metrics,
                include_params=self.args.include_params,
                exclude_params=self.args.exclude_params,
                no_timestamp=self.args.no_timestamp,
                sort_by=self.args.sort_by,
                sort_order=self.args.sort_order,
                precision=precision,
                fill_value=fill_value,
                iso=iso,
                pager=not self.args.no_pager,
                csv=self.args.csv,
                markdown=self.args.markdown,
                only_changed=self.args.only_changed,
                html=self.args.html,
                out=self.args.out,
                open=self.args.open,
            )
        return 0


def add_parser(experiments_subparsers, parent_parser):
    EXPERIMENTS_SHOW_HELP = "Print experiments."
    experiments_show_parser = experiments_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_SHOW_HELP, "exp/show"),
        help=EXPERIMENTS_SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_show_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Show experiments derived from the tip of all Git branches.",
    )
    experiments_show_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Show experiments derived from all Git tags.",
    )
    experiments_show_parser.add_argument(
        "-A",
        "--all-commits",
        action="store_true",
        default=False,
        help="Show experiments derived from all Git commits.",
    )
    experiments_show_parser.add_argument(
        "-n",
        "--num",
        type=int,
        default=1,
        dest="num",
        metavar="<num>",
        help="Show the last `num` commits from HEAD.",
    )
    experiments_show_parser.add_argument(
        "--no-pager",
        action="store_true",
        default=False,
        help="Do not pipe output into a pager.",
    )
    experiments_show_parser.add_argument(
        "--include-metrics",
        action="append",
        default=[],
        help="Include the specified metrics in output table.",
        metavar="<metrics_list>",
    )
    experiments_show_parser.add_argument(
        "--exclude-metrics",
        action="append",
        default=[],
        help="Exclude the specified metrics from output table.",
        metavar="<metrics_list>",
    )
    experiments_show_parser.add_argument(
        "--include-params",
        action="append",
        default=[],
        help="Include the specified params in output table.",
        metavar="<params_list>",
    )
    experiments_show_parser.add_argument(
        "--exclude-params",
        action="append",
        default=[],
        help="Exclude the specified params from output table.",
        metavar="<params_list>",
    )
    experiments_show_parser.add_argument(
        "--param-deps",
        action="store_true",
        default=False,
        help="Show only params that are stage dependencies.",
    )
    experiments_show_parser.add_argument(
        "--sort-by",
        help="Sort related experiments by the specified metric or param.",
        metavar="<metric/param>",
    )
    experiments_show_parser.add_argument(
        "--sort-order",
        help=(
            "Sort order to use with --sort-by."
            " Defaults to ascending ('asc')."
        ),
        choices=("asc", "desc"),
        default="asc",
    )
    experiments_show_parser.add_argument(
        "--no-timestamp",
        action="store_true",
        default=False,
        help="Do not show experiment timestamps.",
    )
    experiments_show_parser.add_argument(
        "--sha",
        action="store_true",
        default=False,
        help="Always show git commit SHAs instead of branch/tag names.",
    )
    experiments_show_parser.add_argument(
        "--json",
        "--show-json",
        action="store_true",
        default=False,
        help="Print output in JSON format instead of a human-readable table.",
    )
    experiments_show_parser.add_argument(
        "--csv",
        "--show-csv",
        action="store_true",
        default=False,
        help="Print output in csv format instead of a human-readable table.",
    )
    experiments_show_parser.add_argument(
        "--md",
        "--show-md",
        action="store_true",
        default=False,
        dest="markdown",
        help="Show tabulated output in the Markdown format (GFM).",
    )
    experiments_show_parser.add_argument(
        "--precision",
        type=int,
        help=(
            "Round metrics/params to `n` digits precision after the decimal "
            f"point. Rounds to {DEFAULT_PRECISION} digits by default."
        ),
        metavar="<n>",
    )
    experiments_show_parser.add_argument(
        "--only-changed",
        action="store_true",
        default=False,
        help=(
            "Only show metrics/params with values varying "
            "across the selected experiments."
        ),
    )
    experiments_show_parser.add_argument(
        "--html",
        action="store_true",
        default=False,
        help="Generate a parallel coordinates plot from the tabulated output.",
    )
    experiments_show_parser.add_argument(
        "-o",
        "--out",
        default=None,
        help="Destination folder to save the HTML to",
        metavar="<path>",
    ).complete = completion.DIR
    experiments_show_parser.add_argument(
        "--open",
        action="store_true",
        default=False,
        help="Open the HTML directly in the browser.",
    )
    experiments_show_parser.set_defaults(func=CmdExperimentsShow)
