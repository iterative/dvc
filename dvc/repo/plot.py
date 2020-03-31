import logging
import os

from dvc.plot import DefaultTemplate

logger = logging.getLogger(__name__)


def create_data_dict(target, typ):
    result = {}
    if typ == "json":
        import json

        with open(target, "r+") as fd:
            data = json.load(fd)
            for d in data:
                d["revision"] = "HEAD"

    result["data"] = {}
    result["data"]["values"] = data
    result["title"] = target
    return result


def plot(repo, targets, typ="json"):
    # TODO how to handle multiple targets
    target = targets[0]
    vega_data_dict = create_data_dict(target, typ)

    # TODO need to pass title, probably need a way to pass additional config
    # from json file

    DefaultTemplate(repo.dvc_dir).save(
        vega_data_dict, os.path.join(repo.root_dir, "viz.html")
    )
