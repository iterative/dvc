import csv
import json
import logging
import os
import re

from funcy import first

from dvc.exceptions import DvcException
from dvc.plot import Template
from dvc.repo import locked
from dvc.utils import format_link

logger = logging.getLogger(__name__)


def _all_dict_of_length_one(data):
    return all([isinstance(e, dict) and len(e) == 1 for e in data])


def _load_from_tree(tree, datafile, default_plot=False):
    if datafile.endswith(".json"):
        with tree.open(datafile, "r") as fobj:
            data = json.load(fobj)
            assert isinstance(data, list)

        if default_plot:
            assert all(len(e) >= 1 for e in data)
            last_key = list(first(data).keys())[-1]
            data = [{"y": d[last_key], "x": i} for i, d in enumerate(data)]
    elif datafile.endswith(".csv"):
        with tree.open(datafile, "r") as fobj:
            if default_plot:
                data = []
                for index, row in enumerate(csv.reader(fobj)):
                    assert len(row) >= 1
                    if index == 0 and len(row) > 1:
                        # skip header
                        continue
                    data.append({"y": row[-1], "x": index})
            else:
                data = [
                    row
                    for row in (csv.DictReader(fobj, skipinitialspace=True))
                ]

    return data


def _load_from_revision(repo, datafile, revision=None, default_plot=False):
    if revision is None:
        revision = "current workspace"
        tree = repo.tree
    else:
        tree = repo.scm.get_tree(revision)

    try:
        data = _load_from_tree(tree, datafile, default_plot)
        for d in data:
            d["revision"] = revision
    except FileNotFoundError:
        logger.warning(
            "File '{}' was not found at: '{}'. It will not be "
            "plotted.".format(datafile, revision)
        )
        data = []
    return data


def _load_from_revisions(repo, datafile, revisions, default_plot=False):
    data = []
    if len(revisions) == 0:
        if repo.scm.is_dirty():
            data.extend(
                _load_from_revision(repo, datafile, "HEAD", default_plot)
            )
        data.extend(
            _load_from_revision(repo, datafile, default_plot=default_plot)
        )
    elif len(revisions) == 1:
        data.extend(
            _load_from_revision(repo, datafile, revisions[0], default_plot)
        )
        data.extend(
            _load_from_revision(repo, datafile, default_plot=default_plot)
        )
    else:
        for rev in revisions:
            data.extend(_load_from_revision(repo, datafile, rev, default_plot))

    if not data:
        raise DvcException(
            "Target metric: '{}' could not be found at any of '{}'".format(
                datafile, ", ".join(revisions)
            )
        )
    return data


def _evaluate_templatepath(repo, template):
    if os.path.exists(template):
        return template
    else:
        # TODO
        logger.debug("Template '{}' not found, checking in plot dir.")
        plots_dir_path = os.path.join(
            repo.plot_templates.templates_dir, template
        )
        if os.path.exists(plots_dir_path):
            return plots_dir_path
        else:
            regex = re.compile(template + ".*")
            for t in os.listdir(repo.plot_templates.templates_dir):
                if regex.match(t):
                    return os.path.join(repo.plot_templates.templates_dir, t)
        raise DvcException("No template found")


@locked
def plot(repo, datafile=None, template=None, revisions=None):
    default_plot = False

    if template is None:
        template_path = os.path.join(
            repo.plot_templates.templates_dir, "default.dvct"
        )
        default_plot = True
    else:
        template_path = _evaluate_templatepath(repo, template)
        # TODO exception
        assert template_path.endswith(".dvct")

    if revisions is None:
        revisions = []

    template_datafiles = Template.parse_data_placeholders(template_path)
    if datafile:
        template_datafiles.add(datafile)

    data = {
        datafile: _load_from_revisions(repo, datafile, revisions, default_plot)
        for datafile in template_datafiles
    }

    result_path = Template.fill(template_path, data, datafile)
    logger.info(
        "Your can see your plot by opening {} in your "
        "browser!".format(
            format_link(
                "file://{}".format(os.path.join(repo.root_dir, result_path))
            )
        )
    )
    return result_path
    # replace DVC_PLOT_DATA in template w
