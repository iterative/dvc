import argparse
import logging

from dvc.command.base import append_doc_link
from dvc.command.base import CmdBase
from dvc.command.base import fix_subparsers
from dvc.exceptions import BadMetricError
from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


def show_metrics(metrics, all_branches=False, all_tags=False):
    """
    Args:
        metrics (list): Where each element is either a `list`
            if an xpath was specified, otherwise a `str`
    """
    # When `metrics` contains a `None` key, it means that some files
    # specified as `targets` in `repo.metrics.show` didn't contain any metrics.
    missing = metrics.pop(None, None)

    for branch, val in metrics.items():
        if all_branches or all_tags:
            logger.info("{branch}:".format(branch=branch))

        for fname, metric in val.items():
            if isinstance(metric, dict):
                lines = list(metric.values())
            elif isinstance(metric, list):
                lines = metric
            else:
                lines = metric.splitlines()

            if len(lines) > 1:
                logger.info("\t{fname}:".format(fname=fname))

                for line in lines:
                    logger.info("\t\t{content}".format(content=line))

            else:
                logger.info("\t{}: {}".format(fname, metric))

    if missing:
        raise BadMetricError(missing)


class CmdMetricsShow(CmdBase):
    def run(self):
        try:
            metrics = self.repo.metrics.show(
                self.args.targets,
                typ=self.args.type,
                xpath=self.args.xpath,
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                recursive=self.args.recursive,
            )

            show_metrics(metrics, self.args.all_branches, self.args.all_tags)
        except DvcException:
            logger.exception("failed to show metrics")
            return 1

        return 0


class CmdMetricsModify(CmdBase):
    def run(self):
        try:
            self.repo.metrics.modify(
                self.args.path, typ=self.args.type, xpath=self.args.xpath
            )
        except DvcException:
            logger.exception("failed to modify metric file settings")
            return 1

        return 0


class CmdMetricsAdd(CmdBase):
    def run(self):
        try:
            self.repo.metrics.add(
                self.args.path, self.args.type, self.args.xpath
            )
        except DvcException:
            msg = "failed to add metric file '{}'".format(self.args.path)
            logger.exception(msg)
            return 1

        return 0


class CmdMetricsRemove(CmdBase):
    def run(self):
        try:
            self.repo.metrics.remove(self.args.path)
        except DvcException:
            msg = "failed to remove metric file '{}'".format(self.args.path)
            logger.exception(msg)
            return 1

        return 0


def _show_diff(diff):
    from texttable import Texttable

    if not diff:
        return "No changes."

    table = Texttable()

    # remove borders to make it easier for users to copy stuff
    table.set_chars(("", "", "", ""))
    table.set_deco(0)

    rows = [["Path", "Metric", "Value", "Change"]]
    for fname, mdiff in diff.items():
        for metric, change in mdiff.items():
            rows.append(
                [
                    fname,
                    metric,
                    change["new"],
                    change.get("diff", "diff not supported"),
                ]
            )
    table.add_rows(rows)
    return table.draw()


class CmdMetricsDiff(CmdBase):
    def run(self):
        try:
            diff = self.repo.metrics.diff(
                a_rev=self.args.a_rev,
                b_rev=self.args.b_rev,
                targets=self.args.targets,
                typ=self.args.type,
                xpath=self.args.xpath,
                recursive=self.args.recursive,
            )

            if self.args.show_json:
                import json

                logger.info(json.dumps(diff))
            else:
                logger.info(_show_diff(diff))

        except DvcException:
            logger.exception("failed to show metrics diff")
            return 1

        return 0


def add_parser(subparsers, parent_parser):
    METRICS_HELP = "Commands to add, manage, collect and display metrics."

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

    METRICS_SHOW_HELP = "Output metric values."
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
        help="Metric files or directories (see -R) to show "
        "(leave empty to display all)",
    )
    metrics_show_parser.add_argument(
        "-t",
        "--type",
        help=(
            "Type of metrics (json/tsv/htsv/csv/hcsv). "
            "It can be detected by the file extension automatically. "
            "Unsupported types will be treated as raw."
        ),
    )
    metrics_show_parser.add_argument(
        "-x", "--xpath", help="json/tsv/htsv/csv/hcsv path."
    )
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

    METRICS_ADD_HELP = "Tag file as a metric file."
    metrics_add_parser = metrics_subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=append_doc_link(METRICS_ADD_HELP, "metrics/add"),
        help=METRICS_ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    metrics_add_parser.add_argument(
        "-t", "--type", help="Type of metrics (raw/json/tsv/htsv/csv/hcsv)."
    )
    metrics_add_parser.add_argument(
        "-x", "--xpath", help="json/tsv/htsv/csv/hcsv path."
    )
    metrics_add_parser.add_argument("path", help="Path to a metric file.")
    metrics_add_parser.set_defaults(func=CmdMetricsAdd)

    METRICS_MODIFY_HELP = "Modify metric file options."
    metrics_modify_parser = metrics_subparsers.add_parser(
        "modify",
        parents=[parent_parser],
        description=append_doc_link(METRICS_MODIFY_HELP, "metrics/modify"),
        help=METRICS_MODIFY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    metrics_modify_parser.add_argument(
        "-t", "--type", help="Type of metrics (raw/json/tsv/htsv/csv/hcsv)."
    )
    metrics_modify_parser.add_argument(
        "-x", "--xpath", help="json/tsv/htsv/csv/hcsv path."
    )
    metrics_modify_parser.add_argument("path", help="Path to a metric file.")
    metrics_modify_parser.set_defaults(func=CmdMetricsModify)

    METRICS_REMOVE_HELP = "Remove files's metric tag."
    metrics_remove_parser = metrics_subparsers.add_parser(
        "remove",
        parents=[parent_parser],
        description=append_doc_link(METRICS_REMOVE_HELP, "metrics/remove"),
        help=METRICS_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    metrics_remove_parser.add_argument("path", help="Path to a metric file.")
    metrics_remove_parser.set_defaults(func=CmdMetricsRemove)

    METRICS_DIFF_HELP = "Show a table of changes between metrics among "
    "versions of the DVC repository."
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
        help=("New Git commit to compare (defaults to the current workspace)"),
    )
    metrics_diff_parser.add_argument(
        "--targets",
        nargs="*",
        help=(
            "Metric files or directories (see -R) to show diff for. "
            "Shows diff for all metric files by default."
        ),
    )
    metrics_diff_parser.add_argument(
        "-t",
        "--type",
        help=(
            "Type of metrics (json/tsv/htsv/csv/hcsv). "
            "It can be detected by the file extension automatically. "
            "Unsupported types will be treated as raw."
        ),
    )
    metrics_diff_parser.add_argument(
        "-x", "--xpath", help="json/tsv/htsv/csv/hcsv path."
    )
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
        "--show-json",
        action="store_true",
        default=False,
        help="Show output in JSON format.",
    )
    metrics_diff_parser.set_defaults(func=CmdMetricsDiff)
