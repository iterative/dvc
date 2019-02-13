from __future__ import unicode_literals

import os
import csv
import json
from jsonpath_rw import parse

import dvc.logger as logger
from dvc.exceptions import (
    OutputDuplicationError,
    DvcException,
    OutputNotFoundError,
    BadMetricError,
    NoMetricsError,
)
from dvc.utils.compat import str, builtin_str, open


def _read_metric_json(fd, json_path):
    parser = parse(json_path)
    return [x.value for x in parser.find(json.load(fd))]


def _do_read_metric_xsv(reader, row, col):
    if col is not None and row is not None:
        return [reader[row][col]]
    elif col is not None:
        return [r[col] for r in reader]
    elif row is not None:
        return reader[row]
    return None


def _read_metric_hxsv(fd, hxsv_path, delimiter):
    col, row = hxsv_path.split(",")
    row = int(row)
    reader = list(csv.DictReader(fd, delimiter=builtin_str(delimiter)))
    return _do_read_metric_xsv(reader, row, col)


def _read_metric_xsv(fd, xsv_path, delimiter):
    col, row = xsv_path.split(",")
    row = int(row)
    col = int(col)
    reader = list(csv.reader(fd, delimiter=builtin_str(delimiter)))
    return _do_read_metric_xsv(reader, row, col)


def _read_metric(path, typ=None, xpath=None):
    ret = None

    if not os.path.exists(path):
        return ret

    try:
        with open(path, "r") as fd:
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
    except Exception:
        logger.error("unable to read metric in '{}'".format(path))

    return ret


def _collect_metrics(self, path, recursive, typ, xpath):
    outs = [out for stage in self.stages() for out in stage.outs]

    if path:
        try:
            outs = self.find_outs_by_path(path, outs=outs, recursive=recursive)
        except OutputNotFoundError:
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


def _read_metrics(self, metrics):
    res = {}
    for out, typ, xpath in metrics:
        assert out.scheme == "local"
        if out.use_cache:
            path = self.cache.local.get(out.checksum)
        else:
            path = out.path

        metric = _read_metric(path, typ=typ, xpath=xpath)
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
    for branch in self.scm.brancher(
        all_branches=all_branches, all_tags=all_tags
    ):
        entries = _collect_metrics(self, path, recursive, typ, xpath)
        metrics = _read_metrics(self, entries)
        if metrics:
            res[branch] = metrics

    if not res:
        if path:
            raise BadMetricError(path)
        raise NoMetricsError()

    return res
