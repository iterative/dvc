import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.exceptions import BadMetricError, DvcException

logger = logging.getLogger(__name__)


DEFAULT_PRECISION = 5


def show_metrics(
    metrics, all_branches=False, all_tags=False, all_commits=False
):
    from flatten_json import flatten

    from dvc.utils.diff import format_dict

    # When `metrics` contains a `None` key, it means that some files
    # specified as `targets` in `repo.metrics.show` didn't contain any metrics.
    missing = metrics.pop(None, None)

    for branch, val in metrics.items():
        if all_branches or all_tags or all_commits:
            logger.info(f"{branch}:")

        for fname, metric in val.items():
            if not isinstance(metric, dict):
                logger.info("\t{}: {}".format(fname, str(metric)))
                continue

            logger.info(f"\t{fname}:")
            for key, value in flatten(format_dict(metric), ".").items():
                logger.info(f"\t\t{key}: {value}")

    if missing:
        raise BadMetricError(missing)


class CmdMetricsShow(CmdBase):
    def run(self):
        try:
            metrics = self.repo.metrics.show(
                self.args.targets,
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
                recursive=self.args.recursive,
            )

            if self.args.show_json:
                import json

                logger.info(json.dumps(metrics))
            else:
                show_metrics(
                    metrics,
                    self.args.all_branches,
                    self.args.all_tags,
                    self.args.all_commits,
                )
        except DvcException:
            logger.exception("failed to show metrics")
            return 1

        return 0


def _show_diff(diff, markdown=False, no_path=False, old=False, precision=None):
    from collections import OrderedDict

    from dvc.utils.diff import table

    if precision is None:
        precision = DEFAULT_PRECISION

    def _round(val):
        if isinstance(val, float):
            return round(val, precision)

        return val

    rows = []
    for fname, mdiff in diff.items():
        sorted_mdiff = OrderedDict(sorted(mdiff.items()))
        for metric, change in sorted_mdiff.items():
            row = [] if no_path else [fname]
            row.append(metric)
            if old:
                row.append(_round(change.get("old")))
            row.append(_round(change["new"]))
            row.append(_round(change.get("diff", "diff not supported")))
            rows.append(row)

    header = [] if no_path else ["Path"]
    header.append("Metric")
    if old:
        header.extend(["Old", "New"])
    else:
        header.append("Value")
    header.append("Change")

    return table(header, rows, markdown)


class CmdMetricsDiff(CmdBase):
    def run(self):
        try:
            diff = self.repo.metrics.diff(
                a_rev=self.args.a_rev,
                b_rev=self.args.b_rev,
                targets=self.args.targets,
                recursive=self.args.recursive,
                all=self.args.all,
            )

            if self.args.show_json:
                import json

                logger.info(json.dumps(diff))
            else:
                table = _show_diff(
                    diff,
                    self.args.show_md,
                    self.args.no_path,
                    self.args.old,
                    precision=self.args.precision,
                )
                if table:
                    logger.info(table)

        except DvcException:
            logger.exception("failed to show metrics diff")
            return 1

        return 0


def add_parser(subparsers, parent_parser):
    METRICS_HELP = "Commands to display and compare metrics."

    metrics_parser = subparsers.add_parser(
        "metrics",
        parents=[parent_parser],
        description=append_doc_link(METRICS_HELP, "metrics"),
        help=METRICS_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    metrics_subparsers = metrics_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc metrics CMD --help` to display command-specific help.",
    )

    fix_subparsers(metrics_subparsers)

    METRICS_SHOW_HELP = "Print metrics, with optional formatting."
    metrics_show_parser = metrics_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(METRICS_SHOW_HELP, "metrics/show"),
        help=METRICS_SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    metrics_show_parser.add_argument(
        "targets",
        nargs="*",
        help=(
            "Limit command scope to these metric files. Using -R, "
            "directories to search metric files in can also be given."
        ),
    ).complete = completion.FILE
    metrics_show_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Show metrics for all branches.",
    )
    metrics_show_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Show metrics for all tags.",
    )
    metrics_show_parser.add_argument(
        "--all-commits",
        action="store_true",
        default=False,
        help="Show metrics for all commits.",
    )
    metrics_show_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    metrics_show_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help=(
            "If any target is a directory, recursively search and process "
            "metric files."
        ),
    )
    metrics_show_parser.set_defaults(func=CmdMetricsShow)

    METRICS_DIFF_HELP = (
        "Show changes in metrics between commits in the DVC repository, or "
        "between a commit and the workspace."
    )
    metrics_diff_parser = metrics_subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(METRICS_DIFF_HELP, "metrics/diff"),
        help=METRICS_DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    metrics_diff_parser.add_argument(
        "a_rev", nargs="?", help="Old Git commit to compare (defaults to HEAD)"
    )
    metrics_diff_parser.add_argument(
        "b_rev",
        nargs="?",
        help="New Git commit to compare (defaults to the current workspace)",
    )
    metrics_diff_parser.add_argument(
        "--targets",
        nargs="*",
        help=(
            "Limit command scope to these metric files. Using -R, "
            "directories to search metric files in can also be given."
        ),
        metavar="<paths>",
    ).complete = completion.FILE
    metrics_diff_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help=(
            "If any target is a directory, recursively search and process "
            "metric files."
        ),
    )
    metrics_diff_parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Show unchanged metrics as well.",
    )
    metrics_diff_parser.add_argument(
        "--show-json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    metrics_diff_parser.add_argument(
        "--show-md",
        action="store_true",
        default=False,
        help="Show tabulated output in the Markdown format (GFM).",
    )
    metrics_diff_parser.add_argument(
        "--old",
        action="store_true",
        default=False,
        help="Show old metric value.",
    )
    metrics_diff_parser.add_argument(
        "--no-path",
        action="store_true",
        default=False,
        help="Don't show metric path.",
    )
    metrics_diff_parser.add_argument(
        "--precision",
        type=int,
        help=(
            "Round metrics to `n` digits precision after the decimal point. "
            f"Rounds to {DEFAULT_PRECISION} digits by default."
        ),
        metavar="<n>",
    )
    metrics_diff_parser.set_defaults(func=CmdMetricsDiff)
