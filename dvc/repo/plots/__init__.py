import csv
import io
import logging
import os
from collections import OrderedDict, defaultdict
from copy import deepcopy
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Set,
    Union,
)

import dpath.options
import dpath.util
from funcy import cached_property, first, project

from dvc.exceptions import DvcException
from dvc.utils import error_handler, errored_revisions, onerror_collect
from dvc.utils.serialize import LOADERS

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.repo import Repo

dpath.options.ALLOW_EMPTY_STRING_KEYS = True

logger = logging.getLogger(__name__)


class PlotMetricTypeError(DvcException):
    def __init__(self, file):
        super().__init__(
            "'{}' - file type error\n"
            "Only JSON, YAML, CSV and TSV formats are supported.".format(file)
        )


class NotAPlotError(DvcException):
    def __init__(self, out):
        super().__init__(
            f"'{out}' is not a known plot. Use `dvc plots modify` to turn it "
            "into one."
        )


class PropsNotFoundError(DvcException):
    pass


@error_handler
def _unpack_dir_files(fs, path, **kwargs):
    return list(fs.find(path))


class Plots:
    def __init__(self, repo):
        self.repo = repo

    def collect(
        self,
        targets: List[str] = None,
        revs: List[str] = None,
        recursive: bool = False,
        onerror: Optional[Callable] = None,
        props: Optional[Dict] = None,
        config_files: Optional[Set[str]] = None,
    ) -> Generator[Dict, None, None]:
        """Collects plots definitions and data sources.

        Generator yielding a structure like:
            {
                revision:
                {
                    "definitions":
                    {
                        "data":
                        {
                            "config_file":
                            {
                                "data":
                                {
                                    plot_id:
                                    {
                                        plot_config
                                    }
                                }
                            }
                        }
                    },
                    "sources":
                    {
                        "data":
                        {
                            "filename":
                            {
                                "data_source": callable loading the data,
                                "props": properties for the file if it is
                                         plots type output
                            }
                        }
                    }
                }

            }
        """
        from dvc.utils.collections import ensure_list

        targets = ensure_list(targets)
        targets = [self.repo.dvcfs.from_os_path(target) for target in targets]

        for rev in self.repo.brancher(revs=revs):
            # .brancher() adds unwanted workspace
            if revs is not None and rev not in revs:
                continue
            rev = rev or "workspace"

            res: Dict = {}
            definitions = _collect_definitions(
                self.repo,
                targets=targets,
                revision=rev,
                onerror=onerror,
                config_files=config_files,
                props=props,
            )
            if definitions:
                res[rev] = {"definitions": definitions}

                data_targets = _get_data_targets(definitions)

                res[rev]["sources"] = self._collect_data_sources(
                    revision=rev,
                    targets=data_targets,
                    recursive=recursive,
                    props=props,
                    onerror=onerror,
                )
            yield res

    @error_handler
    def _collect_data_sources(
        self,
        targets: Optional[List[str]] = None,
        revision: Optional[str] = None,
        recursive: bool = False,
        props: Optional[Dict] = None,
        onerror: Optional[Callable] = None,
    ):
        from dvc.fs.dvc import DVCFileSystem

        fs = DVCFileSystem(repo=self.repo)

        props = props or {}

        plots = _collect_plots(self.repo, targets, revision, recursive)
        res: Dict[str, Any] = {}
        for fs_path, rev_props in plots.items():
            joined_props = {**rev_props, **props}
            res[fs_path] = {"props": joined_props}
            res[fs_path].update(
                {
                    "data_source": partial(
                        parse,
                        fs,
                        fs_path,
                        props=joined_props,
                        onerror=onerror,
                    )
                }
            )
        return res

    def show(
        self,
        targets: List[str] = None,
        revs=None,
        props=None,
        recursive=False,
        onerror=None,
        config_files: Optional[Set[str]] = None,
    ):
        if onerror is None:
            onerror = onerror_collect

        result: Dict[str, Dict] = {}
        for data in self.collect(
            targets,
            revs,
            recursive,
            onerror=onerror,
            props=props,
            config_files=config_files,
        ):
            _resolve_data_sources(data)
            result.update(data)

        errored = errored_revisions(result)
        if errored:
            from dvc.ui import ui

            ui.error_write(
                "DVC failed to load some plots for following revisions: "
                f"'{', '.join(errored)}'."
            )

        return result

    def diff(self, *args, **kwargs):
        from .diff import diff

        return diff(self.repo, *args, **kwargs)

    @staticmethod
    def _unset(out, props):
        missing = list(set(props) - set(out.plot.keys()))
        if missing:
            raise PropsNotFoundError(
                f"display properties {missing} not found in plot '{out}'"
            )

        for prop in props:
            out.plot.pop(prop)

    def modify(self, path, props=None, unset=None):
        from dvc.dvcfile import Dvcfile
        from dvc_render.vega_templates import get_template

        props = props or {}
        template = props.get("template")
        if template:
            get_template(template, self.templates_dir)

        (out,) = self.repo.find_outs_by_path(path)
        if not out.plot and unset is not None:
            raise NotAPlotError(out)

        # This out will become a plot unless it is one already
        if not isinstance(out.plot, dict):
            out.plot = {}

        if unset:
            self._unset(out, unset)

        out.plot.update(props)

        # Empty dict will move it to non-plots
        if not out.plot:
            out.plot = True

        out.verify_metric()

        dvcfile = Dvcfile(self.repo, out.stage.path)
        dvcfile.dump(out.stage, update_lock=False)

    @cached_property
    def templates_dir(self):
        if self.repo.dvc_dir:
            return os.path.join(self.repo.dvc_dir, "plots")


def _is_plot(out: "Output") -> bool:
    return bool(out.plot)


def _resolve_data_sources(plots_data: Dict):
    for value in plots_data.values():
        if isinstance(value, dict):
            if "data_source" in value:
                data_source = value.pop("data_source")
                assert callable(data_source)
                value.update(data_source())
            _resolve_data_sources(value)


def _collect_plots(
    repo: "Repo",
    targets: List[str] = None,
    rev: str = None,
    recursive: bool = False,
) -> Dict[str, Dict]:
    from dvc.repo.collect import collect

    plots, fs_paths = collect(
        repo,
        output_filter=_is_plot,
        targets=targets,
        rev=rev,
        recursive=recursive,
    )

    result = {
        repo.dvcfs.from_os_path(plot.fs_path): _plot_props(plot)
        for plot in plots
    }
    result.update({fs_path: {} for fs_path in fs_paths})
    return result


def _get_data_targets(definitions: Dict):
    result: Set = set()
    if "data" in definitions:
        for content in definitions["data"].values():
            if "data" in content:
                for plot_id, config in content["data"].items():
                    result = result.union(infer_data_sources(plot_id, config))
    return result


def infer_data_sources(plot_id, config=None):
    def _deduplicate(lst: List):
        return list({elem: None for elem in lst}.keys())

    y = config.get("y", None)
    if isinstance(y, dict):
        sources = list(y.keys())
    else:
        sources = [plot_id]

    return _deduplicate(source for source in sources)


def _matches(targets, config_file, plot_id):
    import re

    from dvc.utils.plots import get_plot_id

    if not targets:
        return True

    full_id = get_plot_id(plot_id, config_file)
    if any(
        (re.match(target, plot_id) or re.match(target, full_id))
        for target in targets
    ):
        return True
    return False


def _normpath(path):
    # TODO dvcfs.path.normopath normalizes to windows path on Windows
    # even though other methods work as expected
    import posixpath

    return posixpath.normpath(path)


def _relpath(fs, path):
    # TODO from_os_path changes abs to relative
    # TODO we should be using `dvcfile.relpath` - in case of GitFS (plots diff)
    # and invoking from some subdir `dvcfile.relpath` returns strange long
    # relative paths
    # ("../../../../../../dvc.yaml") - investigate
    return fs.path.relpath(
        fs.path.join("/", fs.from_os_path(path)), fs.path.getcwd()
    )


def _collect_output_plots(
    repo, targets, props, onerror: Optional[Callable] = None
):
    fs = repo.dvcfs
    result: Dict[str, Dict] = {}
    for plot in repo.index.plots:
        plot_props = _plot_props(plot)
        dvcfile = plot.stage.dvcfile
        config_path = _relpath(fs, dvcfile.path)
        wdir_relpath = _relpath(fs, plot.stage.wdir)
        if _matches(targets, config_path, str(plot)):
            unpacked = unpack_if_dir(
                fs,
                _normpath(fs.path.join(wdir_relpath, plot.def_path)),
                props={**plot_props, **props},
                onerror=onerror,
            )

            dpath.util.merge(
                result,
                {"": unpacked},
            )
    return result


def _id_is_path(plot_props=None):
    if not plot_props:
        return True

    y_def = plot_props.get("y")
    return not isinstance(y_def, dict)


def _adjust_sources(fs, plot_props, config_dir):
    new_plot_props = deepcopy(plot_props)
    old_y = new_plot_props.pop("y", {})
    new_y = {}
    for filepath, val in old_y.items():
        new_y[_normpath(fs.path.join(config_dir, filepath))] = val
    new_plot_props["y"] = new_y
    return new_plot_props


def _resolve_definitions(
    fs, targets, props, config_path, definitions, onerror=None
):
    config_dir = fs.path.dirname(config_path)
    result: Dict[str, Dict] = {}
    for plot_id, plot_props in definitions.items():
        if plot_props is None:
            plot_props = {}
        if _id_is_path(plot_props):
            data_path = _normpath(fs.path.join(config_dir, plot_id))
            if _matches(targets, config_path, plot_id):
                unpacked = unpack_if_dir(
                    fs,
                    data_path,
                    props={**plot_props, **props},
                    onerror=onerror,
                )
                dpath.util.merge(
                    result,
                    unpacked,
                )
        else:
            if _matches(targets, config_path, plot_id):
                adjusted_props = _adjust_sources(fs, plot_props, config_dir)
                dpath.util.merge(
                    result, {"data": {plot_id: {**adjusted_props, **props}}}
                )

    return result


def _collect_pipeline_files(repo, targets: List[str], props, onerror=None):
    from dvc.dvcfile import PipelineFile

    result: Dict[str, Dict] = {}
    dvcfiles = {stage.dvcfile for stage in repo.index.stages}
    for dvcfile in dvcfiles:
        if isinstance(dvcfile, PipelineFile):
            dvcfile_path = _relpath(repo.dvcfs, dvcfile.path)
            dvcfile_defs = dvcfile.load().get("plots", {})
            dvcfile_defs_dict: Dict[str, Union[Dict, None]] = {}
            if isinstance(dvcfile_defs, list):
                for elem in dvcfile_defs:
                    if isinstance(elem, str):
                        dvcfile_defs_dict[elem] = None
                    else:
                        k, v = list(elem.items())[0]
                        dvcfile_defs_dict[k] = v
            else:
                dvcfile_defs_dict = dvcfile_defs
            resolved = _resolve_definitions(
                repo.dvcfs,
                targets,
                props,
                dvcfile_path,
                dvcfile_defs_dict,
                onerror=onerror,
            )
            dpath.util.merge(
                result,
                {dvcfile_path: resolved},
            )
    return result


@error_handler
def _collect_definitions(
    repo: "Repo",
    targets=None,
    config_files: Optional[Set[str]] = None,
    props: Dict = None,
    onerror: Optional[Callable] = None,
    **kwargs,
) -> Dict:

    result: Dict = defaultdict(dict)
    props = props or {}

    from dvc.fs.dvc import DVCFileSystem

    fs = DVCFileSystem(repo=repo)

    if not config_files:
        dpath.util.merge(
            result,
            _collect_pipeline_files(repo, targets, props, onerror=onerror),
        )

    if targets or (not targets and not config_files):
        dpath.util.merge(
            result,
            _collect_output_plots(repo, targets, props, onerror=onerror),
        )

    if config_files:
        for path in config_files:
            definitions = parse(fs, path).get("data", {})
            if definitions:
                resolved = _resolve_definitions(
                    repo.dvcfs,
                    targets,
                    props,
                    path,
                    definitions,
                    onerror=onerror,
                )
                dpath.util.merge(
                    result,
                    {path: resolved},
                )

    for target in targets:
        if not result or fs.exists(target):
            unpacked = unpack_if_dir(fs, target, props=props, onerror=onerror)
            dpath.util.merge(result[""], unpacked)

    return dict(result)


def unpack_if_dir(
    fs, path, props: Dict[str, str], onerror: Optional[Callable] = None
):
    result: Dict[str, Dict] = defaultdict(dict)
    if fs.isdir(path):
        unpacked = _unpack_dir_files(fs, path, onerror=onerror)
    else:
        unpacked = {"data": [path]}

    if "data" in unpacked:
        for subpath in unpacked["data"]:
            result["data"].update({subpath: props})
    else:
        result.update(unpacked)

    return dict(result)


@error_handler
def parse(fs, path, props=None, **kwargs):
    props = props or {}
    _, extension = os.path.splitext(path)
    if extension in (".tsv", ".csv"):
        header = props.get("header", True)
        if extension == ".csv":
            return _load_sv(path=path, fs=fs, delimiter=",", header=header)
        return _load_sv(path=path, fs=fs, delimiter="\t", header=header)
    if extension in LOADERS or extension in (".yml", ".yaml"):
        return LOADERS[extension](path=path, fs=fs)
    if extension in (".jpeg", ".jpg", ".gif", ".png", ".svg"):
        with fs.open(path, "rb") as fd:
            return fd.read()
    raise PlotMetricTypeError(path)


def _plot_props(out: "Output") -> Dict:
    from dvc.schema import PLOT_PROPS

    if not (out.plot):
        raise NotAPlotError(out)
    if isinstance(out.plot, list):
        raise DvcException("Multiple plots per data file not supported.")
    if isinstance(out.plot, bool):
        return {}

    return project(out.plot, PLOT_PROPS)


def _load_sv(path, fs, delimiter=",", header=True):
    with fs.open(path, "r") as fd:
        content = fd.read()

    first_row = first(csv.reader(io.StringIO(content)))

    if header:
        reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
    else:
        reader = csv.DictReader(
            io.StringIO(content),
            delimiter=delimiter,
            fieldnames=[str(i) for i in range(len(first_row))],
        )

    fieldnames = reader.fieldnames
    data = list(reader)

    return [
        OrderedDict([(field, data_point[field]) for field in fieldnames])
        for data_point in data
    ]
