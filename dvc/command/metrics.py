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

    def _get_type_xpath(self):
        # backward compatibility
        if self.args.json_path:
            typ = "json"
            xpath = self.args.json_path
        elif self.args.tsv_path:
            typ = "tsv"
            xpath = self.args.tsv_path
        elif self.args.htsv_path:
            typ = "htsv"
            xpath = self.args.htsv_path
        elif self.args.csv_path:
            typ = "csv"
            xpath = self.args.csv_path
        elif self.args.hcsv_path:
            typ = "hcsv"
            xpath = self.args.hcsv_path
        else:
            typ = self.args.type
            xpath = self.args.xpath

        return typ, xpath

    def run(self):
        typ, xpath = self._get_type_xpath()
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
            logger.error("failed to modify metrics")
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
    METRICS_HELP = "Get metrics from all branches."
    metrics_parser = subparsers.add_parser(
        "metrics",
        parents=[parent_parser],
        description=METRICS_HELP,
        help=METRICS_HELP,
    )

    metrics_subparsers = metrics_parser.add_subparsers(
        dest="cmd",
        help="Use dvc metrics CMD --help for command-specific help.",
    )

    fix_subparsers(metrics_subparsers)

    METRICS_SHOW_HELP = "Show metrics."
    metrics_show_parser = metrics_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=METRICS_SHOW_HELP,
        help=METRICS_SHOW_HELP,
    )
    metrics_show_parser.add_argument(
        "path", nargs="?", help="Path to metrics file or directory"
    )
    metrics_show_parser.add_argument(
        "-t", "--type", help="Type of metrics(RAW/JSON/TSV/HTSV/CSV/HCSV)."
    )
    metrics_show_parser.add_argument(
        "-x", "--xpath", help="JSON/TSV/HTSV/CSV/HCSV path."
    )
    metrics_show_group = metrics_show_parser.add_mutually_exclusive_group()
    metrics_show_group.add_argument("--json-path", help="JSON path.")
    metrics_show_group.add_argument(
        "--tsv-path", help="TSV path 'row,column' (e.g. '1,2')."
    )
    metrics_show_group.add_argument(
        "--htsv-path", help="Headed TSV path 'row,column (e.g. 'Name,3')."
    )
    metrics_show_group.add_argument(
        "--csv-path", help="CSV path 'row,column' (e.g. '1,2')."
    )
    metrics_show_group.add_argument(
        "--hcsv-path", help="Headed CSV path 'row,column' (e.g. 'Name,3')."
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
        help="If path is a directory, recursively search and process metric files in path.",
    )
    metrics_show_parser.set_defaults(func=CmdMetricsShow)

    METRICS_ADD_HELP = "Add metrics."
    metrics_add_parser = metrics_subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=METRICS_ADD_HELP,
        help=METRICS_ADD_HELP,
    )
    metrics_add_parser.add_argument(
        "-t", "--type", help="Type of metrics(RAW/JSON/TSV/HTSV/CSV/HCSV)."
    )
    metrics_add_parser.add_argument(
        "-x", "--xpath", help="JSON/TSV/HTSV/CSV/HCSV path."
    )
    metrics_add_parser.add_argument("path", help="Path to metrics file.")
    metrics_add_parser.set_defaults(func=CmdMetricsAdd)

    METRICS_MODIFY_HELP = "Modify metrics."
    metrics_modify_parser = metrics_subparsers.add_parser(
        "modify",
        parents=[parent_parser],
        description=METRICS_MODIFY_HELP,
        help=METRICS_MODIFY_HELP,
    )
    metrics_modify_parser.add_argument(
        "-t", "--type", help="Type of metrics(RAW/JSON/TSV/HTSV/CSV/HCSV)."
    )
    metrics_modify_parser.add_argument(
        "-x", "--xpath", help="JSON/TSV/HTSV/CSV/HCSV path."
    )
    metrics_modify_parser.add_argument("path", help="Metrics file.")
    metrics_modify_parser.set_defaults(func=CmdMetricsModify)

    METRICS_REMOVE_HELP = "Remove metrics."
    metrics_remove_parser = metrics_subparsers.add_parser(
        "remove",
        parents=[parent_parser],
        description=METRICS_REMOVE_HELP,
        help=METRICS_REMOVE_HELP,
    )
    metrics_remove_parser.add_argument("path", help="Path to metrics file.")
    metrics_remove_parser.set_defaults(func=CmdMetricsRemove)
