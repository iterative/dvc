from __future__ import unicode_literals

import argparse
import logging

from dvc.exceptions import DvcException
from dvc.command.base import CmdBase, fix_subparsers, append_doc_link


logger = logging.getLogger(__name__)


def show_metrics(metrics, all_branches=False, all_tags=False):
    """
    Args:
        metrics (list): Where each element is either a `list`
            if an xpath was specified, otherwise a `str`
    """
    for branch, val in metrics.items():
        if all_branches or all_tags:
            logger.info("{branch}:".format(branch=branch))

        for fname, metric in val.items():
            lines = metric if type(metric) is list else metric.splitlines()

            if len(lines) > 1:
                logger.info("\t{fname}:".format(fname=fname))

                for line in lines:
                    logger.info("\t\t{content}".format(content=line))

            else:
                logger.info("\t{}: {}".format(fname, metric))


class CmdMetricsShow(CmdBase):
    def run(self):
        typ = self.args.type
        xpath = self.args.xpath
        try:
            metrics = self.repo.metrics.show(
                self.args.path,
                typ=typ,
                xpath=xpath,
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
        help="Use dvc metrics CMD --help to display command-specific help.",
    )

    fix_subparsers(metrics_subparsers)

    METRICS_SHOW_HELP = "Output metric values."
    metrics_show_parser = metrics_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(METRICS_SHOW_HELP, "metrics-show"),
        help=METRICS_SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    metrics_show_parser.add_argument(
        "path", nargs="?", help="Path to a metric file or a directory."
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
            "If path is a directory, recursively search and process metric "
            "files in path."
        ),
    )
    metrics_show_parser.set_defaults(func=CmdMetricsShow)

    METRICS_ADD_HELP = "Tag file as a metric file."
    metrics_add_parser = metrics_subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=append_doc_link(METRICS_ADD_HELP, "metrics-add"),
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
        description=append_doc_link(METRICS_MODIFY_HELP, "metrics-modify"),
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
        description=append_doc_link(METRICS_REMOVE_HELP, "metrics-remove"),
        help=METRICS_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    metrics_remove_parser.add_argument("path", help="Path to a metric file.")
    metrics_remove_parser.set_defaults(func=CmdMetricsRemove)
