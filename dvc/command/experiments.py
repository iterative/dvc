import argparse
import io
import logging
from collections import OrderedDict
from itertools import groupby
from typing import Iterable, Optional

from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.command.metrics import DEFAULT_PRECISION
from dvc.exceptions import DvcException, InvalidArgumentError

logger = logging.getLogger(__name__)


def _filter_names(
    names: Iterable,
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

    names = {tuple(name.split(".")) for name in names}

    def _filter(filters, update_func):
        filters = {tuple(name.split(".")) for name in filters}
        for length, groups in groupby(filters, len):
            for group in groups:
                matches = [name for name in names if name[:length] == group]
                if not matches:
                    name = ".".join(group)
                    raise InvalidArgumentError(
                        f"'{name}' does not match any known {label}"
                    )
                update_func(matches)

    if include:
        ret = set()
        _filter(include, ret.update)
    else:
        ret = set(names)

    if exclude:
        _filter(exclude, ret.difference_update)

    return {".".join(name) for name in ret}


def _update_names(names, items):
    from flatten_json import flatten

    for name, item in items:
        if isinstance(item, dict):
            item = flatten(item, ".")
            names.update(item.keys())
        else:
            names.add(name)


def _collect_names(all_experiments, **kwargs):
    metric_names = set()
    param_names = set()

    for _, experiments in all_experiments.items():
        for exp in experiments.values():
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

    return sorted(metric_names), sorted(param_names)


def _collect_rows(
    base_rev,
    experiments,
    metric_names,
    param_names,
    precision=DEFAULT_PRECISION,
    no_timestamp=False,
):
    for i, (rev, exp) in enumerate(experiments.items()):
        row = []
        style = None
        queued = "*" if exp.get("queued", False) else ""
        if no_timestamp:
            timestamp = ""
        else:
            commit_time = exp.get("timestamp")
            timestamp = f" ({commit_time})" if commit_time is not None else ""

        if rev == "baseline":
            row.append(f"{base_rev}{timestamp}")
            style = "bold"
        elif i < len(experiments) - 1:
            row.append(f"├── {queued}{rev[:7]}{timestamp}")
        else:
            row.append(f"└── {queued}{rev[:7]}{timestamp}")

        _extend_row(
            row, metric_names, exp.get("metrics", {}).items(), precision
        )
        _extend_row(row, param_names, exp.get("params", {}).items(), precision)

        yield row, style


def _extend_row(row, names, items, precision):
    from flatten_json import flatten

    def _round(val):
        if isinstance(val, float):
            return round(val, precision)

        return val

    if not items:
        row.extend(["-"] * len(names))
        return

    for fname, item in items:
        if isinstance(item, dict):
            item = flatten(item, ".")
        else:
            item = {fname: item}
        for name in names:
            if name in item:
                value = item[name]
                text = str(_round(value)) if value is not None else "-"
                row.append(text)
            else:
                row.append("-")


def _parse_list(param_list):
    ret = []
    for param_str in param_list:
        # we don't care about filename prefixes for show, silently
        # ignore it if provided to keep usage consistent with other
        # metric/param list command options
        _, _, param_str = param_str.rpartition(":")
        ret.extend(param_str.split(","))
    return ret


def _show_experiments(all_experiments, console, **kwargs):
    from rich.table import Table

    from dvc.scm.git import Git

    include_metrics = _parse_list(kwargs.pop("include_metrics", []))
    exclude_metrics = _parse_list(kwargs.pop("exclude_metrics", []))
    include_params = _parse_list(kwargs.pop("include_params", []))
    exclude_params = _parse_list(kwargs.pop("exclude_params", []))

    metric_names, param_names = _collect_names(
        all_experiments,
        include_metrics=include_metrics,
        exclude_metrics=exclude_metrics,
        include_params=include_params,
        exclude_params=exclude_params,
    )

    table = Table()
    table.add_column("Experiment", no_wrap=True)
    for name in metric_names:
        table.add_column(name, justify="right", no_wrap=True)
    for name in param_names:
        table.add_column(name, justify="left")

    for base_rev, experiments in all_experiments.items():
        if Git.is_sha(base_rev):
            base_rev = base_rev[:7]

        for row, _, in _collect_rows(
            base_rev, experiments, metric_names, param_names, **kwargs,
        ):
            table.add_row(*row)

    console.print(table)


class CmdExperimentsShow(CmdBase):
    def run(self):
        from rich.console import Console

        from dvc.utils.pager import pager

        if not self.repo.experiments:
            return 0

        try:
            all_experiments = self.repo.experiments.show(
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
            )

            if self.args.no_pager:
                console = Console()
            else:
                # Note: rich does not currently include a native way to force
                # infinite width for use with a pager
                console = Console(
                    file=io.StringIO(), force_terminal=True, width=9999
                )

            _show_experiments(
                all_experiments,
                console,
                include_metrics=self.args.include_metrics,
                exclude_metrics=self.args.exclude_metrics,
                include_params=self.args.include_params,
                exclude_params=self.args.exclude_params,
                no_timestamp=self.args.no_timestamp,
            )

            if not self.args.no_pager:
                pager(console.file.getvalue())
        except DvcException:
            logger.exception("failed to show experiments")
            return 1

        return 0


class CmdExperimentsCheckout(CmdBase):
    def run(self):
        if not self.repo.experiments:
            return 0

        self.repo.experiments.checkout(self.args.experiment)

        return 0


def _show_diff(
    diff, title="", markdown=False, no_path=False, old=False, precision=None
):
    from dvc.utils.diff import table

    if precision is None:
        precision = DEFAULT_PRECISION

    def _round(val):
        if isinstance(val, float):
            return round(val, precision)

        return val

    rows = []
    for fname, diff_ in diff.items():
        sorted_diff = OrderedDict(sorted(diff_.items()))
        for item, change in sorted_diff.items():
            row = [] if no_path else [fname]
            row.append(item)
            if old:
                row.append(_round(change.get("old")))
            row.append(_round(change["new"]))
            row.append(_round(change.get("diff", "diff not supported")))
            rows.append(row)

    header = [] if no_path else ["Path"]
    header.append(title)
    if old:
        header.extend(["Old", "New"])
    else:
        header.append("Value")
    header.append("Change")

    return table(header, rows, markdown)


class CmdExperimentsDiff(CmdBase):
    def run(self):
        if not self.repo.experiments:
            return 0

        try:
            diff = self.repo.experiments.diff(
                a_rev=self.args.a_rev,
                b_rev=self.args.b_rev,
                all=self.args.all,
            )

            if self.args.show_json:
                import json

                logger.info(json.dumps(diff))
            else:
                diffs = [("metrics", "Metric"), ("params", "Param")]
                for key, title in diffs:
                    table = _show_diff(
                        diff[key],
                        title=title,
                        markdown=self.args.show_md,
                        no_path=self.args.no_path,
                        old=self.args.old,
                        precision=self.args.precision,
                    )
                    if table:
                        logger.info(table)
                        logger.info("")

        except DvcException:
            logger.exception("failed to show experiments diff")
            return 1

        return 0


def add_parser(subparsers, parent_parser):
    EXPERIMENTS_HELP = "Commands to display and compare experiments."

    experiments_parser = subparsers.add_parser(
        "experiments",
        parents=[parent_parser],
        aliases=["exp"],
        description=append_doc_link(EXPERIMENTS_HELP, "experiments"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    experiments_subparsers = experiments_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc experiments CMD --help` to display "
        "command-specific help.",
    )

    fix_subparsers(experiments_subparsers)

    EXPERIMENTS_SHOW_HELP = "Print experiments."
    experiments_show_parser = experiments_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_SHOW_HELP, "experiments/show"),
        help=EXPERIMENTS_SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_show_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Show metrics for all branches.",
    )
    experiments_show_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Show metrics for all tags.",
    )
    experiments_show_parser.add_argument(
        "--all-commits",
        action="store_true",
        default=False,
        help="Show metrics for all commits.",
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
        "--no-timestamp",
        action="store_true",
        default=False,
        help="Do not show experiment timestamps.",
    )
    experiments_show_parser.set_defaults(func=CmdExperimentsShow)

    EXPERIMENTS_CHECKOUT_HELP = "Checkout experiments."
    experiments_checkout_parser = experiments_subparsers.add_parser(
        "checkout",
        parents=[parent_parser],
        description=append_doc_link(
            EXPERIMENTS_CHECKOUT_HELP, "experiments/checkout"
        ),
        help=EXPERIMENTS_CHECKOUT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_checkout_parser.add_argument(
        "experiment", help="Checkout this experiment.",
    )
    experiments_checkout_parser.set_defaults(func=CmdExperimentsCheckout)

    EXPERIMENTS_DIFF_HELP = (
        "Show changes between experiments in the DVC repository."
    )
    experiments_diff_parser = experiments_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_DIFF_HELP, "experiments/diff"),
        help=EXPERIMENTS_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_diff_parser.add_argument(
        "a_rev", nargs="?", help="Old experiment to compare (defaults to HEAD)"
    )
    experiments_diff_parser.add_argument(
        "b_rev",
        nargs="?",
        help="New experiment to compare (defaults to the current workspace)",
    )
    experiments_diff_parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Show unchanged metrics/params as well.",
    )
    experiments_diff_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    experiments_diff_parser.add_argument(
        "--show-md",
        action="store_true",
        default=False,
        help="Show tabulated output in the Markdown format (GFM).",
    )
    experiments_diff_parser.add_argument(
        "--old",
        action="store_true",
        default=False,
        help="Show old metric/param value.",
    )
    experiments_diff_parser.add_argument(
        "--no-path",
        action="store_true",
        default=False,
        help="Don't show metric/param path.",
    )
    experiments_diff_parser.add_argument(
        "--precision",
        type=int,
        help=(
            "Round metrics/params to `n` digits precision after the decimal "
            f"point. Rounds to {DEFAULT_PRECISION} digits by default."
        ),
        metavar="<n>",
    )
    experiments_diff_parser.set_defaults(func=CmdExperimentsDiff)
