import csv
import io
import json
import logging
import os
from collections import OrderedDict

from funcy import first
from ruamel import yaml

from dvc.exceptions import DvcException, PathMissingError
from dvc.template import Template
from dvc.repo import locked

logger = logging.getLogger(__name__)

PAGE_HTML = """<html>
<head>
    <title>dvc plot</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5.10.0"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@4.8.1"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6.5.1"></script>
</head>
<body>
    {divs}
</body>
</html>"""

DIV_HTML = """<div id = "{id}"></div>
<script type = "text/javascript">
    var spec = {vega_json};
    vegaEmbed('#{id}', spec);
</script>"""


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
    def __init__(self, file):
        super().__init__(
            "'{}' - file type error\n"
            "Only json, yaml, csv and tsv types are supported.".format(file)
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


def parse(datafile, content, default_plot=False):
    _, extension = os.path.splitext(datafile.lower())
    if extension == ".json":
        return _parse_json(io.StringIO(content), default_plot)
    elif extension == ".csv":
        return _parse_csv(io.StringIO(content), default_plot)
    elif extension == ".tsv":
        return _parse_csv(io.StringIO(content), default_plot, "\t")
    elif extension == ".yaml":
        return _parse_yaml(io.StringIO(content), default_plot)
    raise PlotMetricTypeError(datafile)


def _load_from_revision(repo, datafile, revision):
    if revision is WORKSPACE_REVISION_NAME:

        def open_datafile():
            return repo.tree.open(datafile, "r")

    else:

        def open_datafile():
            from dvc import api

            return api.open(datafile, repo.root_dir, revision)

    try:
        with open_datafile() as fobj:
            datafile_content = fobj.read()

    except (FileNotFoundError, PathMissingError):
        raise NoMetricOnRevisionError(datafile, revision)

    return datafile_content


def _load_from_revisions(repo, datafile, revisions, default_plot=False):
    data = []
    exceptions = []

    for rev in revisions:
        try:
            content = _load_from_revision(repo, datafile, rev)

            tmp = parse(datafile, content, default_plot)
            for data_point in tmp:
                data_point["rev"] = rev

            data.extend(tmp)

        except NoMetricOnRevisionError as e:
            exceptions.append(e)
        except PlotMetricTypeError:
            raise
        except Exception:
            logger.error("Failed to parse '{}' at '{}.'".format(datafile, rev))
            raise

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
def fill_template(repo, datafile, template_path, revisions):
    default_plot = template_path == repo.plot_templates.default_template

    template_datafiles = _parse_template(template_path, datafile)

    data = {
        datafile: _load_from_revisions(repo, datafile, revisions, default_plot)
        for datafile in template_datafiles
    }
    return Template.fill(template_path, data, datafile)


def plot(
    repo, datafile=None, template=None, revisions=None, fname=None, embed=False
):
    if revisions is None:
        revisions = [WORKSPACE_REVISION_NAME]

    if not datafile and not template:
        raise NoDataOrTemplateProvided()

    template_path = _evaluate_templatepath(repo, template)

    plot_content = fill_template(repo, datafile, template_path, revisions)

    if embed:
        div = DIV_HTML.format(id="plot", vega_json=plot_content)
        plot_content = PAGE_HTML.format(divs=div)

    if not fname:
        fname = _infer_result_file(datafile, template_path, embed)

    with open(fname, "w") as fobj:
        fobj.write(plot_content)
    return fname


def _infer_result_file(datafile, template_path, embed):
    if datafile:
        tmp = datafile
    else:
        tmp = "plot"

    if not embed:
        extension = os.path.splitext(template_path)[1]
    else:
        extension = ".html"

    result_file = os.path.splitext(tmp)[0] + extension

    if result_file == datafile or result_file == template_path:
        raise DvcException(
            "Could not infer plot name, please provide it " "with -f option."
        )
    return result_file


def _parse_template(template_path, datafile):
    template_datafiles = Template.parse_data_placeholders(template_path)
    if datafile:
        if len(template_datafiles) > 1:
            raise TooManyDataSourcesError(datafile, template_datafiles)
        template_datafiles = {datafile}
    return template_datafiles
