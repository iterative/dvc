from __future__ import unicode_literals

import os

from dvc.exceptions import DvcException, OutputDuplicationError


def find_output_by_path(repo, path, outs=None, recursive=False):
    if not outs:
        astages = repo.active_stages()
        outs = [out for stage in astages for out in stage.outs]

    abs_path = os.path.abspath(path)
    if os.path.isdir(abs_path) and recursive:
        matched = [
            out
            for out in outs
            if os.path.abspath(out.path).startswith(abs_path)
        ]
    else:
        matched = [out for out in outs if out.path == abs_path]
        stages = [out.stage.relpath for out in matched]
        if len(stages) > 1:
            raise OutputDuplicationError(path, stages)

    return matched if matched else []


def modify(repo, path, typ=None, xpath=None, delete=False):
    outs = find_output_by_path(repo, path)

    if not outs:
        msg = "unable to find file '{}' in the pipeline".format(path)
        raise DvcException(msg)

    if len(outs) != 1:
        msg = (
            "-R not yet supported for metrics modify. "
            "Make sure only one metric is referred to by '{}'".format(path)
        )
        raise DvcException(msg)

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
