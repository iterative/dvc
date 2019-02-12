from __future__ import unicode_literals

import os
import csv
import json
from jsonpath_rw import parse

import dvc.logger as logger
from dvc.exceptions import OutputDuplicationError, DvcException
from dvc.repo.metrics.modify import find_output_by_path
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
        astages = self.active_stages()
        outs = [out for stage in astages for out in stage.outs]

        if path:
            outs = find_output_by_path(
                self, path, outs=outs, recursive=recursive
            )

        metrics = filter(lambda o: o.metric, outs)
        stages = set()
        entries = []

        for o in metrics:
            if not typ and isinstance(o.metric, dict):
                t = o.metric.get(o.PARAM_METRIC_TYPE, typ)
                x = o.metric.get(o.PARAM_METRIC_XPATH, xpath)
            else:
                t = typ
                x = xpath
            entries.append((o.path, t, x))
            stages.add(o.stage.path)

        if path and not entries:
            if os.path.isdir(path):
                logger.warning(
                    "Path '{path}' is a directory. "
                    "Consider running with '-R'.".format(path=path)
                )
                return {}

        for fname, t, x in entries:
            if stages:
                for stage in stages:
                    self.checkout(stage, force=True)

            rel = os.path.relpath(fname)
            metric = _read_metric(fname, typ=t, xpath=x)
            if not metric:
                continue

            if branch not in res:
                res[branch] = {}

            res[branch][rel] = metric

    for branch, val in res.items():
        if all_branches or all_tags:
            logger.info("{}:".format(branch))
        for fname, metric in val.items():
            logger.info("\t{}: {}".format(fname, metric))

    if res:
        return res

    if path and os.path.isdir(path):
        return res

    if path:
        msg = "file '{}' does not exist, not a metric file or is malformed".format(
            path
        )
    else:
        msg = (
            "no metric files in this repository."
            " use 'dvc metrics add' to add a metric file to track."
        )

    raise DvcException(msg)
