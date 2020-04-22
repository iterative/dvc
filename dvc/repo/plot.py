import csv
import io
import itertools
import json
import logging
import os
from collections import OrderedDict
from copy import copy

from funcy import first, last, cached_property
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


class UnexpectedJsonStructureError(DvcException):
    pass


class JsonParsingError(DvcException):
    def __init__(self, file):
        super().__init__(
            "Failed to infer data structure from '{}'. Did you forget "
            "to specify jsonpath?".format(file)
        )


WORKSPACE_REVISION_NAME = "workspace"


def plot_data(filename, revision, content):
    _, extension = os.path.splitext(filename.lower())
    if extension == ".json":
        return JSONPlotData(filename, revision, content)
    elif extension == ".csv":
        return CSVPlotData(filename, revision, content)
    elif extension == ".tsv":
        return CSVPlotData(filename, revision, content, delimiter="\t")
    elif extension == ".yaml":
        return YAMLPLotData(filename, revision, content)
    raise PlotMetricTypeError(filename)


def _filter_fields(data_points, fieldnames=None, fields=None, **kwargs):
    if not fields:
        return data_points, fieldnames
    assert isinstance(fields, set)

    new_data = []
    for data_point in data_points:
        new_dp = copy(data_point)
        to_del = set(data_point.keys()) - fields
        for key in to_del:
            del new_dp[key]
            if fieldnames and key in fieldnames:
                fieldnames.remove(key)
        new_data.append(new_dp)
    return new_data, fieldnames


def _transform_to_default_data(
    data_points, fieldnames=None, default_plot=False, **kwargs
):
    if not default_plot:
        return data_points, fieldnames

    new_data = []
    if fieldnames:
        y = last(fieldnames)
    else:
        y = last(list(first(data_points).keys()))

    for index, data_point in enumerate(data_points):
        new_data.append({"x": index, "y": data_point[y]})
    return new_data, ["x", "y"]


def _apply_path(data, fieldnames=None, path=None, **kwargs):
    if not path:
        return data, fieldnames

    import jsonpath_ng

    found = jsonpath_ng.parse(path).find(data)
    first_datum = first(found)
    if (
        len(found) == 1
        and isinstance(first_datum.value, list)
        and isinstance(first(first_datum.value), dict)
    ):
        data_points = first_datum.value
        fieldnames = list(first(data_points).keys())
    elif len(first_datum.path.fields) == 1:
        field_name = first(first_datum.path.fields)
        data_points = [{field_name: datum.value} for datum in found]
    else:
        raise DvcException("Could not parse data for path '{}'".format(path))

    if not isinstance(data_points, list) or not (
        isinstance(first(data_points), dict)
    ):
        raise UnexpectedJsonStructureError("Unable to parse")

    return data_points, fieldnames


class PlotData:
    def __init__(self, filename, revision, content, **kwargs):
        self.filename = filename
        self.revision = revision
        self.content = content
        self.fieldnames = None

    @property
    def raw(self):
        raise NotImplementedError

    def _processors(self):
        return [_filter_fields, _transform_to_default_data]

    def to_datapoints(self, **kwargs):
        data = self.raw
        fieldnames = self.fieldnames

        for data_proc in self._processors():
            data, fieldnames = data_proc(data, fieldnames, **kwargs)

        for data_point in data:
            data_point["rev"] = self.revision
        return data


class JSONPlotData(PlotData):
    @cached_property
    def raw(self):
        return json.loads(self.content, object_pairs_hook=OrderedDict)

    def _processors(self):
        parent_processors = super(JSONPlotData, self)._processors()
        return [_apply_path] + parent_processors


class CSVPlotData(PlotData):
    def __init__(self, filename, revision, content, delimiter=","):
        super(CSVPlotData, self).__init__(filename, revision, content)
        self.delimiter = delimiter

    @cached_property
    def raw(self):
        first_row = first(csv.reader(io.StringIO(self.content)))

        if len(first_row) == 1:
            reader = csv.DictReader(
                io.StringIO(self.content),
                delimiter=self.delimiter,
                fieldnames=["value"],
            )
        else:
            reader = csv.DictReader(
                io.StringIO(self.content),
                skipinitialspace=True,
                delimiter=self.delimiter,
            )

        self.fieldnames = reader.fieldnames
        return [row for row in reader]


class YAMLPLotData(PlotData):
    @cached_property
    def raw(self):
        return yaml.parse(io.StringIO(self.content))


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

    return plot_data(datafile, revision, datafile_content)


def _load_from_revisions(repo, datafile, revisions):
    data = []
    exceptions = []

    for rev in revisions:
        try:
            data.append(_load_from_revision(repo, datafile, rev))
        except NoMetricOnRevisionError as e:
            exceptions.append(e)
        except PlotMetricTypeError:
            raise
        except UnexpectedJsonStructureError:
            raise JsonParsingError(datafile)
        except Exception:
            logger.error("Failed to parse '{}' at '{}'.".format(datafile, rev))
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
def fill_template(
    repo, datafile, template_path, revisions, fields=None, path=None
):
    default_plot = template_path == repo.plot_templates.default_template

    template_datafiles = _parse_template(template_path, datafile)

    template_data = {}
    for datafile in template_datafiles:
        plot_datas = _load_from_revisions(repo, datafile, revisions)
        template_data[datafile] = list(
            itertools.chain.from_iterable(
                [
                    pd.to_datapoints(
                        fields=fields, default_plot=default_plot, path=path
                    )
                    for pd in plot_datas
                ]
            )
        )

    return Template.fill(template_path, template_data, datafile)


def plot(
    repo,
    datafile=None,
    template=None,
    revisions=None,
    fname=None,
    fields=None,
    path=None,
    embed=False,
):
    if revisions is None:
        revisions = [WORKSPACE_REVISION_NAME]

    if not datafile and not template:
        raise NoDataOrTemplateProvided()

    template_path = _evaluate_templatepath(repo, template)

    plot_content = fill_template(
        repo, datafile, template_path, revisions, fields, path
    )

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
