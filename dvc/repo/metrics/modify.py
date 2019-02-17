from __future__ import unicode_literals

from dvc.exceptions import DvcException


def modify(repo, path, typ=None, xpath=None, delete=False):
    outs = repo.find_outs_by_path(path)
    assert len(outs) == 1
    out = outs[0]

    if out.scheme != "local":
        msg = "output '{}' scheme '{}' is not supported for metrics"
        raise DvcException(msg.format(out.path, out.path_info["scheme"]))

    if typ is not None:
        typ = typ.lower().strip()
        if typ not in ["raw", "json", "csv", "tsv", "hcsv", "htsv"]:
            msg = "metric type '{}' is not supported"
            raise DvcException(msg.format(typ))
        if not isinstance(out.metric, dict):
            out.metric = {}
        out.metric[out.PARAM_METRIC_TYPE] = typ

    if xpath is not None:
        if not isinstance(out.metric, dict):
            out.metric = {}
        out.metric[out.PARAM_METRIC_XPATH] = xpath

    if delete:
        out.metric = None

    out.verify_metric()

    out.stage.dump()
