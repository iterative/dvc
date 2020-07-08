import argparse
import io
import logging

from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


def _update_names(names, items):
    from flatten_json import flatten

    for name, item in items:
        if isinstance(item, dict):
            item = flatten(item, ".")
            names.update(item.keys())
        else:
            names.add(name)


def _collect_names(experiments):
    metric_names = set()
    param_names = set()

    for exp in experiments.values():
        _update_names(metric_names, exp.get("metrics", {}).items())
        _update_names(param_names, exp.get("params", {}).items())

    return sorted(metric_names), sorted(param_names)


def _collect_rows(
    experiments, metric_names, param_names, include_rev=False, precision=None
):
    from flatten_json import flatten
    from dvc.command.metrics import DEFAULT_PRECISION

    if precision is None:
        precision = DEFAULT_PRECISION

    def _round(val):
        if isinstance(val, float):
            return round(val, precision)

        return val

    def _extend(row, names, items):
        for fname, item in items:
            if isinstance(item, dict):
                item = flatten(item, ".")
            else:
                item = {fname: item}
            for name in names:
                if name in item:
                    row.append(str(_round(item[name])))
                else:
                    row.append("-")

    for rev, exp in experiments.items():
        row = []
        if include_rev:
            row.append(rev)
        else:
            row.append(None)

        _extend(row, metric_names, exp.get("metrics", {}).items())
        _extend(row, param_names, exp.get("params", {}).items())

        yield row


def _show_experiments(
    experiments,
    all_branches=False,
    all_tags=False,
    all_commits=False,
    precision=None,
):
    from rich.console import Console
    from rich.table import Table
    from dvc.utils.pager import pager

    metric_names, param_names = _collect_names(experiments)
    include_rev = all_branches or all_tags or all_commits

    table = Table(show_lines=True)
    table.add_column("Commit")
    for name in metric_names:
        table.add_column(name, justify="right")
    for name in param_names:
        table.add_column(name, justify="left")

    for row in _collect_rows(
        experiments,
        metric_names,
        param_names,
        include_rev=include_rev,
        precision=precision,
    ):
        table.add_row(*row)

    # Note: rich does not currently include a native way to force infinite
    # width for use with a pager
    console = Console(file=io.StringIO(), force_terminal=True, width=9999)
    console.print(table)
    pager(console.file.getvalue())


class CmdExperimentsShow(CmdBase):
    def run(self):
        try:
            experiments = self.repo.experiments.show(
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
            )

            _show_experiments(
                experiments,
                self.args.all_branches,
                self.args.all_tags,
                self.args.all_commits,
            )
        except DvcException:
            logger.exception("failed to show experiments")
            return 1

        return 0


def add_parser(subparsers, parent_parser):
    EXPERIMENTS_HELP = "Commands to display and compare experiments."

    experiments_parser = subparsers.add_parser(
        "experiments",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_HELP, "experiments"),
        help=EXPERIMENTS_HELP,
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
    experiments_show_parser.set_defaults(func=CmdExperimentsShow)
