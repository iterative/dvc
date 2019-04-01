from __future__ import unicode_literals

import os
import csv
import json
from jsonpath_rw import parse

import dvc.logger as logger
from dvc.exceptions import OutputNotFoundError, BadMetricError, NoMetricsError
from dvc.utils.compat import builtin_str, open, StringIO, csv_reader


def _read_metric_json(fd, json_path):
    parser = parse(json_path)
    return [x.value for x in parser.find(json.load(fd))]


def _get_values(row):
    if isinstance(row, dict):
        return list(row.values())
    else:
        return row


def _do_read_metric_xsv(reader, row, col):
    if col is not None and row is not None:
        return [reader[row][col]]
    elif col is not None:
        return [r[col] for r in reader]
    elif row is not None:
        return _get_values(reader[row])
    return [_get_values(r) for r in reader]


def _read_metric_hxsv(fd, hxsv_path, delimiter):
    indices = hxsv_path.split(",")
    row = indices[0]
    row = int(row) if row else None
    col = indices[1] if len(indices) > 1 and indices[1] else None
    reader = list(csv.DictReader(fd, delimiter=builtin_str(delimiter)))
    return _do_read_metric_xsv(reader, row, col)


def _read_metric_xsv(fd, xsv_path, delimiter):
    indices = xsv_path.split(",")
    row = indices[0]
    row = int(row) if row else None
    col = int(indices[1]) if len(indices) > 1 and indices[1] else None
    reader = list(csv.reader(fd, delimiter=builtin_str(delimiter)))
    return _do_read_metric_xsv(reader, row, col)


def _read_typed_metric(typ, xpath, fd):
    if typ == "json":
        ret = _read_metric_json(fd, xpath)
    elif typ == "csv":
        ret = _read_metric_xsv(fd, xpath, ",")
    elif typ == "tsv":
        ret = _read_metric_xsv(fd, xpath, "\t")
    elif typ == "hcsv":
        ret = _read_metric_hxsv(fd, xpath, ",")
    elif typ == "htsv":
        ret = _read_metric_hxsv(fd, xpath, "\t")
    else:
        ret = fd.read().strip()
    return ret


def _format_csv(content, delimiter):
    """Format delimited text to have same column width.

    Args:
        content (str): The content of a metric.
        delimiter (str): Value separator

    Returns:
        str: Formatted content.

    Example:

        >>> content = (
            "value_mse,deviation_mse,data_set\n"
            "0.421601,0.173461,train\n"
            "0.67528,0.289545,testing\n"
            "0.671502,0.297848,validation\n"
        )
        >>> _format_csv(content, ",")

        "value_mse  deviation_mse   data_set\n"
        "0.421601   0.173461        train\n"
        "0.67528    0.289545        testing\n"
        "0.671502   0.297848        validation\n"
    """
    reader = csv_reader(StringIO(content), delimiter=builtin_str(delimiter))
    rows = [row for row in reader]
    max_widths = [max(map(len, column)) for column in zip(*rows)]

    lines = [
        " ".join(
            "{entry:{width}}".format(entry=entry, width=width + 2)
            for entry, width in zip(row, max_widths)
        )
        for row in rows
    ]

    return "\n".join(lines)


def _format_output(content, typ):
    """Tabularize the content according to its type.

    Args:
        content (str): The content of a metric.
        typ (str): The type of metric -- (raw|json|tsv|htsv|csv|hcsv).

    Returns:
        str: Content in a raw or tabular format.
    """

    if "csv" in str(typ):
        return _format_csv(content, delimiter=",")

    if "tsv" in str(typ):
        return _format_csv(content, delimiter="\t")

    return content


def _read_metric(fd, typ=None, xpath=None, rel_path=None, branch=None):
    typ = typ.lower().strip() if typ else typ
    try:
        if xpath:
            return _read_typed_metric(typ, xpath.strip(), fd)
        else:
            return _format_output(fd.read().strip(), typ)
    # Json path library has to be replaced or wrapped in
    # order to fix this too broad except clause.
    except Exception:
        logger.warning(
            "unable to read metric in '{}' in branch '{}'".format(
                rel_path, branch
            ),
            parse_exception=True,
        )
        return None


def _collect_metrics(self, path, recursive, typ, xpath, branch):
    """Gather all the metric outputs.

    Args:
        path (str): Path to a metric file or a directory.
        recursive (bool): If path is a directory, do a recursive search for
            metrics on the given path.
        typ (str): The type of metric to search for, could be one of the
            following (raw|json|tsv|htsv|csv|hcsv).
        xpath (str): Path to search for.
        branch (str): Branch to look up for metrics.

    Returns:
        list(tuple): (output, typ, xpath)
            - output:
            - typ:
            - xpath:
    """
    outs = [out for stage in self.stages() for out in stage.outs]

    if path:
        try:
            outs = self.find_outs_by_path(path, outs=outs, recursive=recursive)
        except OutputNotFoundError:
            logger.debug(
                "stage file not for found for '{}' in branch '{}'".format(
                    path, branch
                )
            )
            return []

    res = []
    for o in outs:
        if not o.metric:
            continue

        if not typ and isinstance(o.metric, dict):
            t = o.metric.get(o.PARAM_METRIC_TYPE, typ)
            x = o.metric.get(o.PARAM_METRIC_XPATH, xpath)
        else:
            t = typ
            x = xpath

        res.append((o, t, x))

    return res


def _read_metrics_filesystem(path, typ, xpath, rel_path, branch):
    if not os.path.exists(path):
        return None
    with open(path, "r") as fd:
        return _read_metric(
            fd, typ=typ, xpath=xpath, rel_path=rel_path, branch=branch
        )


def _read_metrics(self, metrics, branch):
    """Read the content of each metric file and format it.

    Args:
        metrics (list): List of metric touples
        branch (str): Branch to look up for metrics.

    Returns:
        A dict mapping keys with metrics path name and content.
        For example:

        {'metric.csv': ("value_mse  deviation_mse   data_set\n"
                        "0.421601   0.173461        train\n"
                        "0.67528    0.289545        testing\n"
                        "0.671502   0.297848        validation\n")}
    """
    res = {}
    for out, typ, xpath in metrics:
        assert out.scheme == "local"
        if not typ:
            typ = os.path.splitext(out.path.lower())[1].replace(".", "")
        if out.use_cache:
            metric = _read_metrics_filesystem(
                self.cache.local.get(out.checksum),
                typ=typ,
                xpath=xpath,
                rel_path=out.rel_path,
                branch=branch,
            )
        else:
            fd = self.tree.open(out.path)
            metric = _read_metric(
                fd, typ=typ, xpath=xpath, rel_path=out.rel_path, branch=branch
            )

        if not metric:
            continue

        res[out.rel_path] = metric

    return res


def show(
    self,
    path=None,
    typ=None,
    xpath=None,
    all_branches=False,
    all_tags=False,
    recursive=False,
):
    res = {}

    for branch in self.brancher(all_branches=all_branches, all_tags=all_tags):
        entries = _collect_metrics(self, path, recursive, typ, xpath, branch)
        metrics = _read_metrics(self, entries, branch)
        if metrics:
            res[branch] = metrics

    if not res:
        if path:
            raise BadMetricError(path)
        raise NoMetricsError()

    return res
