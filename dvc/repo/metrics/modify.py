from __future__ import unicode_literals

import os

from dvc.exceptions import DvcException


def modify(repo, path, typ=None, xpath=None, delete=False):
    outs = repo.find_outs_by_path(path)
    assert len(outs) == 1
    out = outs[0]

    if out.scheme != "local":
        msg = "output '{}' scheme '{}' is not supported for metrics"
        raise DvcException(msg.format(out.path, out.path_info["scheme"]))

    if typ:
        if not isinstance(out.metric, dict):
            out.metric = {}
        out.metric[out.PARAM_METRIC_TYPE] = typ

    if xpath:
        if not isinstance(out.metric, dict):
            out.metric = {}
        out.metric[out.PARAM_METRIC_XPATH] = xpath

    if delete:
        out.metric = None

    out._verify_metric()

    out.stage.dump()
