from __future__ import unicode_literals

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase, fix_subparsers


class CmdMetricsShow(CmdBase):
    def _show(self, metrics):
        for branch, val in metrics.items():
            if self.args.all_branches or self.args.all_tags:
                logger.info("{}:".format(branch))

            for fname, metric in val.items():
                logger.info("\t{}: {}".format(fname, metric))

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

            self._show(metrics)
        except DvcException:
            logger.error("failed to show metrics")
            return 1

        return 0


class CmdMetricsModify(CmdBase):
    def run(self):
        try:
            self.repo.metrics.modify(
                self.args.path, typ=self.args.type, xpath=self.args.xpath
            )
        except DvcException:
            logger.error("failed to modify metric file settings")
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
            logger.error(msg)
            return 1

        return 0


class CmdMetricsRemove(CmdBase):
    def run(self):
        try:
            self.repo.metrics.remove(self.args.path)
        except DvcException:
            msg = "failed to remove metric file '{}'".format(self.args.path)
            logger.error(msg)
            return 1

        return 0


def add_parser(subparsers, parent_parser):
    METRICS_HELP = (
        "A set of commands to add, manage, collect and display project "
        "metrics.\ndocumentation: https://man.dvc.org/metrics"
    )
    metrics_parser = subparsers.add_parser(
        "metrics",
        parents=[parent_parser],
        description=METRICS_HELP,
        help=METRICS_HELP,
    )

    metrics_subparsers = metrics_parser.add_subparsers(
        dest="cmd",
        help="Use dvc metrics CMD --help to display command-specific help.",
    )

    fix_subparsers(metrics_subparsers)

    METRICS_SHOW_HELP = (
        "Output metric values.\n"
        "documentation: https://man.dvc.org/metrics-show"
    )
    metrics_show_parser = metrics_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=METRICS_SHOW_HELP,
        help=METRICS_SHOW_HELP,
    )
    metrics_show_parser.add_argument(
        "path", nargs="?", help="Path to a metric file or a directory."
    )
    metrics_show_parser.add_argument(
        "-t", "--type", help="Type of metrics (raw/json/tsv/htsv/csv/hcsv)."
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

    METRICS_ADD_HELP = (
        "Tag file as a metric file.\n"
        "documentation: https://man.dvc.org/metrics-add"
    )
    metrics_add_parser = metrics_subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=METRICS_ADD_HELP,
        help=METRICS_ADD_HELP,
    )
    metrics_add_parser.add_argument(
        "-t", "--type", help="Type of metrics (raw/json/tsv/htsv/csv/hcsv)."
    )
    metrics_add_parser.add_argument(
        "-x", "--xpath", help="json/tsv/htsv/csv/hcsv path."
    )
    metrics_add_parser.add_argument("path", help="Path to a metric file.")
    metrics_add_parser.set_defaults(func=CmdMetricsAdd)

    METRICS_MODIFY_HELP = (
        "Modify metric file options.\n"
        "documentation: https://man.dvc.org/metrics-modify "
    )
    metrics_modify_parser = metrics_subparsers.add_parser(
        "modify",
        parents=[parent_parser],
        description=METRICS_MODIFY_HELP,
        help=METRICS_MODIFY_HELP,
    )
    metrics_modify_parser.add_argument(
        "-t", "--type", help="Type of metrics (raw/json/tsv/htsv/csv/hcsv)."
    )
    metrics_modify_parser.add_argument(
        "-x", "--xpath", help="json/tsv/htsv/csv/hcsv path."
    )
    metrics_modify_parser.add_argument("path", help="Path to a metric file.")
    metrics_modify_parser.set_defaults(func=CmdMetricsModify)

    METRICS_REMOVE_HELP = (
        "Remove files's metric tag.\n"
        "documentation: https://man.dvc.org/metrics-remove"
    )

    metrics_remove_parser = metrics_subparsers.add_parser(
        "remove",
        parents=[parent_parser],
        description=METRICS_REMOVE_HELP,
        help=METRICS_REMOVE_HELP,
    )
    metrics_remove_parser.add_argument("path", help="Path to a metric file.")
    metrics_remove_parser.set_defaults(func=CmdMetricsRemove)
