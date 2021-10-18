import csv
import io
import logging
import os
from collections import OrderedDict
from typing import TYPE_CHECKING, Callable, Dict, Generator, List, Optional

from funcy import cached_property, first, project

from dvc.exceptions import DvcException
from dvc.render.vega import PlotMetricTypeError
from dvc.utils import (
    error_handler,
    errored_revisions,
    onerror_collect,
    relpath,
)
from dvc.utils.serialize import LOADERS

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.types import DvcPath

logger = logging.getLogger(__name__)


class NotAPlotError(DvcException):
    def __init__(self, out):
        super().__init__(
            f"'{out}' is not a known plot. Use `dvc plots modify` to turn it "
            "into one."
        )


class PropsNotFoundError(DvcException):
    pass


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
        from dvc.fs.repo import RepoFileSystem

        fs = RepoFileSystem(self.repo)
        plots = _collect_plots(self.repo, targets, revision, recursive)
        res = {}
        for path_info, rev_props in plots.items():

            if fs.isdir(path_info):
                plot_files = []
                for pi in fs.walk_files(path_info):
                    plot_files.append((pi, relpath(pi, self.repo.root_dir)))
            else:
                plot_files = [
                    (path_info, relpath(path_info, self.repo.root_dir))
                ]

            props = props or {}

            for path, repo_path in plot_files:
                joined_props = {**rev_props, **props}
                res[repo_path] = {"props": joined_props}
                res[repo_path].update(
                    parse(
                        fs,
                        path,
                        props=joined_props,
                        onerror=onerror,
                    )
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

        data: Dict[str, Dict] = {}
        for rev_data in self.collect(
            targets, revs, recursive, onerror=onerror, props=props
        ):
            data.update(rev_data)

        errored = errored_revisions(data)
        if errored:
            from dvc.ui import ui

            ui.error_write(
                "DVC failed to load some plots for following revisions: "
                f"'{', '.join(errored)}'."
            )

        return data

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

        props = props or {}
        template = props.get("template")
        if template:
            self.templates.load(template)

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
    def templates(self):
        from .template import PlotTemplates

        return PlotTemplates(self.repo.dvc_dir)


def _is_plot(out: "Output") -> bool:
    return bool(out.plot) or bool(out.live)


def _collect_plots(
    repo: "Repo",
    targets: List[str] = None,
    rev: str = None,
    recursive: bool = False,
) -> Dict["DvcPath", Dict]:
    from dvc.repo.collect import collect

    plots, path_infos = collect(
        repo,
        output_filter=_is_plot,
        targets=targets,
        rev=rev,
        recursive=recursive,
    )

    result = {plot.path_info: _plot_props(plot) for plot in plots}
    result.update({path_info: {} for path_info in path_infos})
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
