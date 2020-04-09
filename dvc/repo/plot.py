import csv
import json
import logging
import os

from funcy import first

from dvc.exceptions import DvcException
from dvc.plot import Template
from dvc.repo import locked

logger = logging.getLogger(__name__)


def _all_dict_of_length_one(data):
    return all([isinstance(e, dict) and len(e) == 1 for e in data])


# TODO test parsing
def _load_from_tree(tree, datafile, default_plot=False):
    if datafile.endswith(".json"):
        data = _parse_json(datafile, default_plot, tree)

    elif datafile.endswith(".csv"):
        data = _parse_csv(datafile, default_plot, tree)

    return data


def _parse_csv(datafile, default_plot, tree):
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
                row for row in (csv.DictReader(fobj, skipinitialspace=True))
            ]
    return data


def _parse_json(datafile, default_plot, tree):
    with tree.open(datafile, "r") as fobj:
        data = json.load(fobj)
        assert isinstance(data, list)
    if default_plot:
        assert all(len(e) >= 1 for e in data)
        last_key = list(first(data).keys())[-1]
        data = [{"y": d[last_key], "x": i} for i, d in enumerate(data)]
    return data


def _load_from_revision(repo, datafile, revision=None, default_plot=False):
    if revision is None:
        revision = "current"
        tree = repo.tree
    else:
        tree = repo.scm.get_tree(revision)

    try:
        data = _load_from_tree(tree, datafile, default_plot)
        for d in data:
            d["rev"] = revision
    except FileNotFoundError:
        logger.warning(
            "File '{}' was not found at: '{}'. It will not be "
            "plotted.".format(datafile, revision)
        )
        data = []
    return data


def _load_from_revisions(repo, datafile, revisions, default_plot=False):
    # TODO test
    data = []
    if len(revisions) == 0:
        # TODO implement status for file
        if repo.scm.is_dirty():
            logger.warning("Repo is dirty, extending with HEAD data")
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
    # TODO test
    if os.path.exists(template):
        return template
    else:
        return repo.plot_templates.get_template(template)


@locked
def plot(repo, datafile=None, template=None, revisions=None):
    if template is None:
        template_path = repo.plot_templates.default_template
    else:
        template_path = _evaluate_templatepath(repo, template)
        # TODO exception
        assert template_path.endswith(".dvct")

    default_plot = (
        True
        if template_path == repo.plot_templates.default_template
        else False
    )

    if revisions is None:
        revisions = []

    template_datafiles = Template.parse_data_placeholders(template_path)

    if datafile:
        if len(template_datafiles) > 1:
            # TODO
            raise DvcException("Don't know which datafile to replace")
        template_datafiles = {datafile}

    data = {
        datafile: _load_from_revisions(repo, datafile, revisions, default_plot)
        for datafile in template_datafiles
    }

    result_path = Template.fill(template_path, data, datafile)
    logger.info("file://{}".format(os.path.join(repo.root_dir, result_path)))
    return result_path
