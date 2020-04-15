import csv
import json
import logging
import os
from collections import OrderedDict

from funcy import first
from ruamel import yaml

from dvc.exceptions import DvcException, PathMissingError
from dvc.plot import Template, NoDataForTemplateError
from dvc.repo import locked

logger = logging.getLogger(__name__)


class NoMetricInHistoryError(DvcException):
    def __init__(self, path, revisions):
        super().__init__(
            "Could not find '{}' on any of the revisions "
            "'{}'".format(path, ", ".join(revisions))
        )


class NoMetricOnRevisionError(DvcException):
    def __init__(self, path, revision):
        self.path = path
        self.revision = revision
        super().__init__(
            "Could not find '{}' on revision " "'{}'".format(path, revision)
        )


class TooManyDataSourcesError(DvcException):
    def __init__(self, datafile, template_datafiles):
        super().__init__(
            "Unable to reason which of possible data sources: '{}' "
            "should be replaced with '{}'".format(
                ", ".join(template_datafiles), datafile
            )
        )


class NoDataOrTemplateProvided(DvcException):
    def __init__(self):
        super().__init__("Datafile or template is not specified.")


class PlotMetricTypeError(DvcException):
    def __init__(self, path):
        super().__init__(
            "'{}' - file type error\n"
            "Only json, yaml, csv and tsv types are supported.".format(path)
        )


WORKSPACE_REVISION_NAME = "workspace"


def _parse(data, default_plot):
    assert isinstance(data, list)
    if default_plot:
        assert all(len(e) >= 1 for e in data)
        last_key = list(first(data).keys())[-1]
        data = [{"y": d[last_key], "x": i} for i, d in enumerate(data)]
    return data


def _parse_yaml(fobj, default_plot):
    data = yaml.load(fobj)

    return _parse(data, default_plot)


def _parse_json(fobj, default_plot):
    data = json.load(fobj, object_pairs_hook=OrderedDict)

    return _parse(data, default_plot)


def _parse_csv(fobj, default_plot, delimiter=","):
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


def _load_from(fobj, datafile, default_plot=False):
    filename = datafile.lower()
    if filename.endswith(".json"):
        data = _parse_json(fobj, default_plot)
    elif filename.endswith(".csv"):
        data = _parse_csv(fobj, default_plot)
    elif filename.endswith(".tsv"):
        data = _parse_csv(fobj, default_plot, "\t")
    elif filename.endswith(".yaml"):
        data = _parse_yaml(fobj, default_plot)
    else:
        raise PlotMetricTypeError(datafile)

    return data


def _load_from_revision(repo, datafile, revision, default_plot=False):
    try:
        if revision is WORKSPACE_REVISION_NAME:

            def open_datafile():
                return repo.tree.open(datafile, "r")

        else:

            def open_datafile():
                from dvc import api

                return api.open(datafile, repo.root_dir, revision)

        with open_datafile() as fobj:
            data = _load_from(fobj, datafile, default_plot)
            for d in data:
                d["rev"] = revision
    except (FileNotFoundError, PathMissingError):
        raise NoMetricOnRevisionError(datafile, revision)
    return data


def _load_from_revisions(repo, datafile, revisions, default_plot=False):
    data = []
    exceptions = []

    if len(revisions) <= 1:
        if len(revisions) == 0 and repo.scm.is_dirty():
            revisions.append("HEAD")
        revisions.append(WORKSPACE_REVISION_NAME)

    for rev in revisions:
        try:
            data.extend(
                _load_from_revision(
                    repo, datafile, rev, default_plot=default_plot
                )
            )
        except NoMetricOnRevisionError as e:
            exceptions.append(e)

    if not data and exceptions:
        raise NoMetricInHistoryError(datafile, revisions)
    else:
        for e in exceptions:
            logger.warning(
                "File '{}' was not found at: '{}'. It will not be "
                "plotted.".format(e.path, e.revision)
            )
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

    if not datafile and not template:
        raise NoDataOrTemplateProvided()

    template_path = _evaluate_templatepath(repo, template)

    default_plot = template_path == repo.plot_templates.default_template

    template_datafiles = _parse_template(template_path, datafile)
    data = {
        datafile: _load_from_revisions(repo, datafile, revisions, default_plot)
        for datafile in template_datafiles
    }

    if not file:
        if datafile:
            file = datafile
        else:
            file = first(template_datafiles)
        if not file:
            raise NoDataForTemplateError(template_path)

        file = "".join(file.split(".")[:-1] or file) + ".html"

    result_path = Template.fill(template_path, data, file, datafile)
    return result_path


def _parse_template(template_path, datafile):
    template_datafiles = Template.parse_data_placeholders(template_path)
    if datafile:
        if len(template_datafiles) > 1:
            raise TooManyDataSourcesError(datafile, template_datafiles)
        template_datafiles = {datafile}
    return template_datafiles
