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


def _collect_names(all_experiments):
    metric_names = set()
    param_names = set()

    for _, experiments in all_experiments.items():
        for exp in experiments.values():
            _update_names(metric_names, exp.get("metrics", {}).items())
            _update_names(param_names, exp.get("params", {}).items())

    return sorted(metric_names), sorted(param_names)


def _collect_rows(
    base_rev, experiments, metric_names, param_names, precision=None
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

    for i, (rev, exp) in enumerate(experiments.items()):
        row = []
        style = None
        if rev == "baseline":
            row.append(f"{base_rev}")
            style = "bold"
        elif i < len(experiments) - 1:
            row.append(f"├── {rev[:7]}")
        else:
            row.append(f"└── {rev[:7]}")

        _extend(row, metric_names, exp.get("metrics", {}).items())
        _extend(row, param_names, exp.get("params", {}).items())

        yield row, style


def _show_experiments(all_experiments, console, precision=None):
    from rich.table import Table
    from dvc.scm.git import Git

    metric_names, param_names = _collect_names(all_experiments)

    table = Table(row_styles=["white", "bright_white"])
    table.add_column("Experiment", header_style="black on grey93")
    for name in metric_names:
        table.add_column(
            name, justify="right", header_style="black on cornsilk1"
        )
    for name in param_names:
        table.add_column(
            name, justify="left", header_style="black on light_cyan1"
        )

    for base_rev, experiments in all_experiments.items():
        if Git.is_sha(base_rev):
            base_rev = base_rev[:7]

        for row, style, in _collect_rows(
            base_rev,
            experiments,
            metric_names,
            param_names,
            precision=precision,
        ):
            table.add_row(*row, style=style)

    console.print(table)


class CmdExperimentsShow(CmdBase):
    def run(self):
        from rich.console import Console
        from dvc.utils.pager import pager

        try:
            all_experiments = self.repo.experiments.show(
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
            )

            # Note: rich does not currently include a native way to force
            # infinite width for use with a pager
            console = Console(
                file=io.StringIO(), force_terminal=True, width=9999
            )

            _show_experiments(all_experiments, console)

            pager(console.file.getvalue())
        except DvcException:
            logger.exception("failed to show experiments")
            return 1

        return 0


class CmdExperimentsCheckout(CmdBase):
    def run(self):
        self.repo.experiments.checkout(
            self.args.experiment, force=self.args.force
        )

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
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Overwrite your current workspace with changes from the "
        "experiment.",
    )
    experiments_checkout_parser.add_argument(
        "experiment", help="Checkout this experiment.",
    )
    experiments_checkout_parser.set_defaults(func=CmdExperimentsCheckout)
