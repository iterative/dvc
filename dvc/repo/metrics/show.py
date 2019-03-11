from __future__ import unicode_literals

import os
import csv
import json
from jsonpath_rw import parse

import dvc.logger as logger
from dvc.exceptions import OutputNotFoundError, BadMetricError, NoMetricsError
from dvc.utils.compat import builtin_str, open


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


def _read_metric(fd, typ=None, xpath=None, rel_path=None, branch=None):
    typ = typ.lower().strip() if typ else typ
    try:
        if xpath:
            return _read_typed_metric(typ, xpath.strip(), fd)
        else:
            return fd.read().strip()
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
    res = {}
    for out, typ, xpath in metrics:
        assert out.scheme == "local"
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
