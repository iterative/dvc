import csv
import json
import logging
import os

from funcy import first

from dvc.exceptions import DvcException
from dvc.plot import Template
from dvc.repo import locked

logger = logging.getLogger(__name__)


class NoMetricsInHistoryError(DvcException):
    def __init__(self, path, revisions):
        super().__init__(
            "Could not find '{}' on any of the revisions: "
            "'{}'".format(path, ", ".join(revisions))
        )


class TooManyDataSourcesError(DvcException):
    def __init__(self, datafile, template_datafiles):
        super().__init__(
            "Unable to reason which of possible data sources: '{}' "
            "should be replaced with '{}'".format(
                ", ".join(template_datafiles), datafile
            )
        )


def _load_from_tree(tree, datafile, default_plot=False):
    if datafile.endswith(".json"):
        data = _parse_json(datafile, default_plot, tree)

    elif datafile.endswith(".csv"):
        data = _parse_csv(datafile, default_plot, tree)
    elif datafile.endswith(".tsv"):
        data = _parse_csv(datafile, default_plot, tree, "\t")
    else:
        raise DvcException(
            "Could not deduct file type from file: '{}'".format(datafile)
        )

    return data


def _parse_csv(datafile, default_plot, tree, delimiter=","):
    with tree.open(datafile, "r") as fobj:
        if default_plot:
            data = []
            for index, row in enumerate(csv.reader(fobj, delimiter=delimiter)):
                assert len(row) >= 1
                if index == 0 and len(row) > 1:
                    # skip header
                    continue
                data.append({"y": row[-1], "x": index})
        else:
            data = [
                row
                for row in (
                    csv.DictReader(
                        fobj, skipinitialspace=True, delimiter=delimiter
                    )
                )
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
    data = []

    if len(revisions) <= 1:
        data.extend(
            _load_from_revision(repo, datafile, default_plot=default_plot)
        )

        if len(revisions) == 0 and repo.scm.is_dirty():
            revisions.append("HEAD")

    for rev in revisions:
        data.extend(
            _load_from_revision(repo, datafile, rev, default_plot=default_plot)
        )

    if not data:
        raise NoMetricsInHistoryError(datafile, revisions)
    return data


def _evaluate_templatepath(repo, template=None):
    if not template:
        return repo.plot_templates.default_template

    if os.path.exists(template):
        return template
    return repo.plot_templates.get_template(template)


@locked
def plot(repo, datafile=None, template=None, revisions=None, file=None):
    if revisions is None:
        revisions = []

    template_path = _evaluate_templatepath(repo, template)

    default_plot = template_path == repo.plot_templates.default_template

    template_datafiles = _parse_template(template_path, datafile)
    data = {
        datafile: _load_from_revisions(repo, datafile, revisions, default_plot)
        for datafile in template_datafiles
    }

    result_path = Template.fill(
        template_path, data, datafile, result_path=file
    )
    logger.info("file://{}".format(os.path.join(repo.root_dir, result_path)))
    return result_path


def _parse_template(template_path, datafile):
    template_datafiles = Template.parse_data_placeholders(template_path)
    if datafile:
        if len(template_datafiles) > 1:
            raise TooManyDataSourcesError(datafile, template_datafiles)
        template_datafiles = {datafile}
    return template_datafiles
