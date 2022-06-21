import csv
import io
import logging
import os
from collections import OrderedDict
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
)

from funcy import cached_property, first, project

from dvc.exceptions import DvcException
from dvc.utils import error_handler, errored_revisions, onerror_collect
from dvc.utils.serialize import LOADERS

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.repo import Repo

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
    ) -> Generator[Dict, None, None]:
        """Collects all props and data for plots.

        Generator yielding a structure like:
            {rev: {plots.csv: {
                props: {x: ..., "header": ..., ...},
                data: "unstructured data (as stored for given extension)",
            }}}
        """
        from dvc.utils.collections import ensure_list

        targets = ensure_list(targets)
        for rev in self.repo.brancher(revs=revs):
            # .brancher() adds unwanted workspace
            if revs is not None and rev not in revs:
                continue
            rev = rev or "workspace"
            yield {
                rev: self._collect_from_revision(
                    revision=rev,
                    targets=targets,
                    recursive=recursive,
                    onerror=onerror,
                    props=props,
                )
            }

    @error_handler
    def _collect_from_revision(
        self,
        targets: Optional[List[str]] = None,
        revision: Optional[str] = None,
        recursive: bool = False,
        onerror: Optional[Callable] = None,
        props: Optional[Dict] = None,
    ):
        from dvc.fs.dvc import DvcFileSystem

        fs = DvcFileSystem(repo=self.repo)
        plots = _collect_plots(self.repo, targets, revision, recursive)
        res: Dict[str, Any] = {}
        for fs_path, rev_props in plots.items():
            base = os.path.join(*fs.path.relparts(fs_path, fs.fs.root_marker))
            if fs.isdir(fs_path):
                plot_files = []
                unpacking_res = _unpack_dir_files(fs, fs_path, onerror=onerror)
                if "data" in unpacking_res:
                    for pi in unpacking_res.get(  # pylint: disable=E1101
                        "data"
                    ):
                        plot_files.append(
                            (
                                pi,
                                os.path.join(
                                    base, *fs.path.relparts(pi, fs_path)
                                ),
                            )
                        )
                else:
                    res[base] = unpacking_res
            else:
                plot_files = [(fs_path, base)]

            props = props or {}

            for path, repo_path in plot_files:
                joined_props = {**rev_props, **props}
                res[repo_path] = {"props": joined_props}
                res[repo_path].update(
                    {
                        "data_source": partial(
                            parse,
                            fs,
                            path,
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
    ):
        if onerror is None:
            onerror = onerror_collect

        result: Dict[str, Dict] = {}
        for data in self.collect(
            targets, revs, recursive, onerror=onerror, props=props
        ):
            assert len(data) == 1
            revision_data = first(data.values())
            if "data" in revision_data:
                for path_data in revision_data["data"].values():
                    result_source = path_data.pop("data_source", None)
                    if result_source:
                        path_data.update(result_source())
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
    return bool(out.plot) or bool(out.live)


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
    if extension in (".jpeg", ".jpg", ".gif", ".png"):
        with fs.open(path, "rb") as fd:
            return fd.read()
    raise PlotMetricTypeError(path)


def _plot_props(out: "Output") -> Dict:
    from dvc.schema import PLOT_PROPS

    if not (out.plot or out.live):
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
