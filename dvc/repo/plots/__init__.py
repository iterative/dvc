import csv
import io
import os
from collections import defaultdict
from collections.abc import Iterator
from copy import deepcopy
from functools import partial
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

import dpath
import dpath.options
from funcy import first, ldistinct, project, reraise

from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.utils import error_handler, errored_revisions
from dvc.utils.objects import cached_property
from dvc.utils.serialize import PARSERS, EncodingError
from dvc.utils.threadpool import ThreadPoolExecutor
from dvc_render.image import ImageRenderer

if TYPE_CHECKING:
    from dvc.fs import FileSystem
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.types import DictStrAny, StrPath

dpath.options.ALLOW_EMPTY_STRING_KEYS = True

logger = logger.getChild(__name__)


def onerror_collect(result: dict, exception: Exception, *args, **kwargs):
    logger.debug("", exc_info=True)  # noqa: LOG014
    result["error"] = exception


SUPPORTED_IMAGE_EXTENSIONS = ImageRenderer.EXTENSIONS


class PlotMetricTypeError(DvcException):
    def __init__(self, file):
        super().__init__(
            f"'{file}' - file type error\n"
            "Only JSON, YAML, CSV and TSV formats are supported."
        )


class NotAPlotError(DvcException):
    def __init__(self, out):
        super().__init__(
            f"'{out}' is not a known plot. Use `dvc plots modify` to turn it into one."
        )


class PropsNotFoundError(DvcException):
    pass


@error_handler
def _unpack_dir_files(fs, path, **kwargs):
    ret = list(fs.find(path))
    if not ret:
        # This will raise FileNotFoundError if it is a broken symlink or TreeError
        next(iter(fs.ls(path)), None)
    return ret


class Plots:
    def __init__(self, repo):
        self.repo = repo

    def collect(
        self,
        targets: Optional[list[str]] = None,
        revs: Optional[list[str]] = None,
        recursive: bool = False,
        onerror: Optional[Callable] = None,
        props: Optional[dict] = None,
    ) -> Iterator[dict]:
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
        from dvc.repo.experiments.brancher import switch_repo
        from dvc.utils.collections import ensure_list

        targets = ensure_list(targets)
        targets = [self.repo.dvcfs.from_os_path(target) for target in targets]

        if revs is None:
            revs = ["workspace"]
        else:
            revs = list(revs)
            if "workspace" in revs:
                # reorder revs to match repo.brancher ordering
                revs.remove("workspace")
                revs = ["workspace", *revs]
        for rev in revs:
            with switch_repo(self.repo, rev) as (repo, _):
                res: dict = {}
                definitions = _collect_definitions(
                    repo,
                    targets=targets,
                    revision=rev,
                    onerror=onerror,
                    props=props,
                )
                if definitions:
                    res[rev] = {"definitions": definitions}

                    data_targets = _get_data_targets(definitions)

                    res[rev]["sources"] = self._collect_data_sources(
                        repo,
                        targets=data_targets,
                        recursive=recursive,
                        props=props,
                        onerror=onerror,
                    )
                yield res

    @error_handler
    def _collect_data_sources(
        self,
        repo: "Repo",
        targets: Optional[list[str]] = None,
        recursive: bool = False,
        props: Optional[dict] = None,
        onerror: Optional[Callable] = None,
    ):
        fs = repo.dvcfs

        props = props or {}

        plots = _collect_plots(repo, targets, recursive)
        res: dict[str, Any] = {}
        for fs_path, rev_props in plots.items():
            joined_props = rev_props | props
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
        targets: Optional[list[str]] = None,
        revs=None,
        props=None,
        recursive=False,
        onerror=None,
    ):
        if onerror is None:
            onerror = onerror_collect

        result: dict[str, dict] = {}
        for data in self.collect(
            targets,
            revs,
            recursive,
            onerror=onerror,
            props=props,
        ):
            short_rev = "workspace"
            if rev := getattr(self.repo.fs, "rev", None):
                short_rev = rev[:7]
            _resolve_data_sources(data, short_rev, cache=True)
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
        out.stage.dump(update_lock=False)

    @cached_property
    def templates_dir(self) -> Optional[str]:
        if self.repo.dvc_dir:
            return os.path.join(self.repo.dvc_dir, "plots")
        return None


def _is_plot(out: "Output") -> bool:
    return bool(out.plot)


def _resolve_data_sources(plots_data: dict, rev: str, cache: bool = False):
    from dvc.progress import Tqdm

    values = list(plots_data.values())
    to_resolve = []
    while values:
        value = values.pop()
        if isinstance(value, dict):
            if "data_source" in value:
                to_resolve.append(value)
            values.extend(value.values())

    def resolve(value):
        data_source = value.pop("data_source")
        assert callable(data_source)
        value.update(data_source(cache=cache))

    if not to_resolve:
        return

    executor = ThreadPoolExecutor(
        max_workers=min(16, 4 * cpu_count()),
        thread_name_prefix="resolve_data",
        cancel_on_error=True,
    )
    with executor:
        iterable = executor.imap_unordered(resolve, to_resolve)
        with Tqdm(
            iterable,
            total=len(to_resolve),
            desc=f"Reading plot's data from {rev}",
            unit="files",
            unit_scale=False,
        ) as progress_iterable:
            list(progress_iterable)


def _collect_plots(
    repo: "Repo",
    targets: Optional[list[str]] = None,
    recursive: bool = False,
) -> dict[str, dict]:
    from dvc.repo.collect import collect

    plots, fs_paths = collect(
        repo,
        output_filter=_is_plot,
        targets=targets,
        recursive=recursive,
    )

    result = {
        repo.dvcfs.from_os_path(plot.fs_path): _plot_props(plot) for plot in plots
    }
    result.update({fs_path: {} for fs_path in fs_paths})
    return result


def _get_data_targets(definitions: dict):
    result: set = set()
    if "data" in definitions:
        for content in definitions["data"].values():
            if "data" in content:
                for plot_id, config in content["data"].items():
                    result = result.union(infer_data_sources(plot_id, config))
    return result


def infer_data_sources(plot_id, config=None):
    y = config.get("y", None) if config else None

    if isinstance(y, dict):
        sources = list(y.keys())
    else:
        sources = [plot_id]

    x = config.get("x", None) if config else None
    if isinstance(x, dict):
        sources.append(first(x.keys()))

    return ldistinct(source for source in sources)


def _matches(targets, config_file, plot_id):
    import re

    from dvc.utils.plots import get_plot_id

    if not targets:
        return True

    full_id = get_plot_id(plot_id, config_file)
    return any(
        (re.match(target, plot_id) or re.match(target, full_id)) for target in targets
    )


def _normpath(path):
    # TODO dvcfs.normopath normalizes to windows path on Windows
    # even though other methods work as expected
    import posixpath

    return posixpath.normpath(path)


def _relpath(fs, path):
    # TODO from_os_path changes abs to relative
    # TODO we should be using `dvcfile.relpath` - in case of GitFS (plots diff)
    # and invoking from some subdir `dvcfile.relpath` returns strange long
    # relative paths
    # ("../../../../../../dvc.yaml") - investigate
    return fs.relpath(fs.join("/", fs.from_os_path(path)), fs.getcwd())


def _collect_output_plots(repo, targets, props, onerror: Optional[Callable] = None):
    fs = repo.dvcfs
    result: dict[str, dict] = {}
    for plot in repo.index.plots:
        plot_props = _plot_props(plot)
        dvcfile = plot.stage.dvcfile
        config_path = _relpath(fs, dvcfile.path)
        wdir_relpath = _relpath(fs, plot.stage.wdir)
        if _matches(targets, config_path, str(plot)):
            unpacked = unpack_if_dir(
                fs,
                _normpath(fs.join(wdir_relpath, plot.def_path)),
                props=plot_props | props,
                onerror=onerror,
            )

            dpath.merge(result, {"": unpacked})
    return result


def _id_is_path(plot_props=None):
    if not plot_props:
        return True

    y_def = plot_props.get("y")
    return not isinstance(y_def, dict)


def _adjust_sources(fs, plot_props, config_dir):
    new_plot_props = deepcopy(plot_props)
    for axis in ["x", "y"]:
        x_is_inferred = axis == "x" and (
            axis not in new_plot_props or isinstance(new_plot_props[axis], str)
        )
        if x_is_inferred:
            continue
        old = new_plot_props.pop(axis, {})
        new = {}
        for filepath, val in old.items():
            new[_normpath(fs.join(config_dir, filepath))] = val
        new_plot_props[axis] = new
    return new_plot_props


def _resolve_definitions(
    fs: "FileSystem",
    targets: list[str],
    props: dict[str, Any],
    config_path: "StrPath",
    definitions: "DictStrAny",
    onerror: Optional[Callable[[Any], Any]] = None,
):
    config_path = os.fspath(config_path)
    config_dir = fs.dirname(config_path)
    result: dict[str, dict] = {}

    plot_ids_parents = [
        _normpath(fs.join(config_dir, plot_id)) for plot_id in definitions
    ]
    for plot_id, plot_props in definitions.items():
        if plot_props is None:
            plot_props = {}
        if _id_is_path(plot_props):
            data_path = _normpath(fs.join(config_dir, plot_id))
            if _matches(targets, config_path, plot_id):
                unpacked = unpack_if_dir(
                    fs, data_path, props=plot_props | props, onerror=onerror
                )
                # use config for parent directory with most specific definition
                if unpacked.get("data"):
                    unpacked["data"] = {
                        k: v
                        for k, v in unpacked["data"].items()
                        if _closest_parent(fs, k, plot_ids_parents) == data_path
                    }
                dpath.merge(result, unpacked)
        elif _matches(targets, config_path, plot_id):
            adjusted_props = _adjust_sources(fs, plot_props, config_dir)
            dpath.merge(result, {"data": {plot_id: adjusted_props | props}})

    return result


def _closest_parent(fs, path, parents):
    best_result = ""
    for parent in parents:
        common_path = fs.commonpath([path, parent])
        if len(common_path) > len(best_result):
            best_result = common_path
    return best_result


def _collect_pipeline_files(repo, targets: list[str], props, onerror=None):
    result: dict[str, dict] = {}
    top_plots = repo.index._plots
    for dvcfile, plots_def in top_plots.items():
        dvcfile_path = _relpath(repo.dvcfs, dvcfile)
        dvcfile_defs_dict: dict[str, Union[dict, None]] = {}
        for elem in plots_def:
            if isinstance(elem, str):
                dvcfile_defs_dict[elem] = None
            else:
                assert elem
                k, v = next(iter(elem.items()))
                dvcfile_defs_dict[k] = v

        resolved = _resolve_definitions(
            repo.dvcfs, targets, props, dvcfile_path, dvcfile_defs_dict, onerror=onerror
        )
        dpath.merge(result, {dvcfile_path: resolved})
    return result


@error_handler
def _collect_definitions(
    repo: "Repo",
    targets: list[str],
    props: Optional[dict] = None,
    onerror: Optional[Callable] = None,
    **kwargs,
) -> dict:
    result: dict = defaultdict(dict)
    props = props or {}

    fs = repo.dvcfs
    dpath.merge(result, _collect_pipeline_files(repo, targets, props, onerror=onerror))

    dpath.merge(result, _collect_output_plots(repo, targets, props, onerror=onerror))

    for target in targets:
        if not result or fs.exists(target):
            unpacked = unpack_if_dir(fs, target, props=props, onerror=onerror)
            dpath.merge(result[""], unpacked)

    return dict(result)


def unpack_if_dir(fs, path, props: dict[str, str], onerror: Optional[Callable] = None):
    result: dict[str, dict] = defaultdict(dict)
    if fs.isdir(path):
        unpacked = _unpack_dir_files(fs, path, onerror=onerror)
    else:
        unpacked = {"data": [path]}

    if "data" in unpacked:
        for subpath in unpacked["data"]:
            result["data"].update({subpath: props.copy()})
    else:
        result.update(unpacked)

    return dict(result)


@error_handler
def parse(fs, path, props=None, **fs_kwargs):
    props = props or {}
    _, extension = os.path.splitext(path)
    if extension in SUPPORTED_IMAGE_EXTENSIONS:
        with fs.open(path, mode="rb", **fs_kwargs) as fd:
            return fd.read()

    if extension not in PARSERS.keys() | {".yml", ".yaml", ".csv", ".tsv"}:
        raise PlotMetricTypeError(path)

    with reraise(UnicodeDecodeError, EncodingError(path, "utf8")):
        with fs.open(path, mode="r", encoding="utf8", **fs_kwargs) as fd:
            contents = fd.read()

    if extension in (".csv", ".tsv"):
        header = props.get("header", True)
        delim = "\t" if extension == ".tsv" else ","
        return _load_sv(contents, delimiter=delim, header=header)
    return PARSERS[extension](contents, path)


def _plot_props(out: "Output") -> dict:
    from dvc.schema import PLOT_PROPS

    if not (out.plot):
        raise NotAPlotError(out)
    if isinstance(out.plot, list):
        raise DvcException("Multiple plots per data file not supported.")
    if isinstance(out.plot, bool):
        return {}

    return project(out.plot, PLOT_PROPS)


def _load_sv(content, delimiter=",", header=True):
    if header:
        reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
    else:
        first_row = first(csv.reader(io.StringIO(content)))
        reader = csv.DictReader(
            io.StringIO(content),
            delimiter=delimiter,
            fieldnames=[str(i) for i in range(len(first_row))],
        )
    return list(reader)
