import logging
import os
from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple, Union

from funcy.seqs import cat
from rich.align import Align
from rich.console import Console, render_group
from rich.markdown import Markdown
from rich.padding import Padding
from rich.rule import Rule
from rich.style import Style
from rich.table import Table
from rich.text import Text

from dvc.command import completion
from dvc.command.base import CmdBase
from dvc.dvcfile import PIPELINE_FILE
from dvc.exceptions import DvcException
from dvc.repo.graph import build_graph
from dvc.repo.trie import build_outs_trie
from dvc.stage import PipelineStage, Stage

if TYPE_CHECKING:
    import networkx as nx

logger = logging.getLogger(__name__)

DVC_BLUE = "#88D5E2"
DVC_LIGHT_ORANGE = "#F8A689"
FORK = "⎇  "
PIPE = " | "
MAX_TEXT_LENGTH = 120


def pluralize(key: str, value: int) -> str:
    assert value
    return key + "s" if value > 1 else key


def get_info(stage: "Stage") -> List[Tuple[str, int]]:
    num_metrics = len([out for out in stage.outs if out.metric])
    num_plots = len([out for out in stage.outs if out.plot])
    return [
        ("dep", len(stage.deps)),
        ("out", len(stage.outs) - num_metrics - num_plots),
        ("metric", num_metrics),
        ("plot", num_plots),
    ]


def sizeof_fmt(size: int, suffix: str = "B", decimal_places: int = 1) -> str:
    unit = ""
    for unit in ["", "Ki", "Mi", "Gi"]:
        if size < 1024.0 or unit == "GiB":
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}{suffix}"


def generate_description(stage: "Stage") -> str:
    def part_desc(outs, show_size=True) -> str:
        desc = ""
        for index, out in enumerate(outs):
            text = ", " if index != 0 else ""
            text += out.def_path
            size = out.hash_info.size
            if size and show_size:
                text += f" ({sizeof_fmt(size)})"
            desc += text
        return desc

    if not stage.deps and not stage.outs:
        return "No outputs or dependencies."

    if not stage.outs and stage.deps:
        return "Stage depends on " + part_desc(stage.deps, show_size=False)

    plots_and_metrics = [out for out in stage.outs if out.plot or out.metric]
    outs = [out for out in stage.outs if not (out.plot or out.metric)]

    outs_desc = ""
    if outs:
        outs_desc = "Produces " + part_desc(outs)

    other_outs_desc = ""
    if plots_and_metrics:
        other_outs_desc = "Generates " + part_desc(plots_and_metrics, False)

    return ", ".join(filter(None, [outs_desc, other_outs_desc]))


def prepare_info(stats: List[Tuple[str, int]]) -> List[Text]:
    ret = []
    for key, value in stats:
        if not value:
            continue
        if ret and ret[-1] != PIPE:
            ret.append(PIPE)
        ret.append(Text.styled(str(value), style="blue"))
        ret.append(Text.styled(f" {pluralize(key, value)}", style="dim blue"))
    return ret


def prepare_graph_text(
    stage: "Stage", graph: "nx.DiGraph" = None
) -> Optional[Text]:
    text = None
    if graph:
        predecessors = graph.predecessors(stage)
        fork = Text(FORK, style="bold blue")

        def gen_name(up, down, *args):
            # if they are both in the same file, let's only show up the names
            use_name = up.path == down.path and isinstance(up, PipelineStage)
            return prepare_stage_name(up, link=False, use_name=use_name)

        fragments = []
        for idx, predecessor in enumerate(predecessors):
            if predecessors is stage:
                continue
            if idx > 0:
                fragments.append(", ")
            fragments.append(fork)
            fragments.append(gen_name(predecessor, stage))

        if fragments:
            text = Text()
            for fragment in fragments:
                text.append(fragment)
            text.truncate(MAX_TEXT_LENGTH, overflow="ellipsis")

    return text


def prepare_stage_name(stage: "Stage", link=False, use_name=False) -> Text:
    title = Text(overflow="fold")
    address = stage.addressing

    linked = Style(link="file://" + os.path.abspath(stage.relpath))
    file_style = Style(color=DVC_BLUE, bold=True, underline=False)
    name_style = None
    if link:
        name_style = linked
        file_style += linked

    if isinstance(stage, PipelineStage):
        from_pwd = stage.name == address
        if from_pwd or use_name:
            title.append(stage.name, style=name_style)
        else:
            title.append(stage.relpath, style=file_style)
            title.append(f":{stage.name}")
    else:
        title.append(address, style=file_style)
    return title


def prepare_description(stage: Stage) -> Union[Markdown, Text]:
    style = "blue"
    if stage.desc:
        # not sure if this was ever intended, but markdown looks nice
        return Markdown(stage.desc, style=style)

    description = generate_description(stage)
    text = Text(description.strip(), style=style)
    text.truncate(MAX_TEXT_LENGTH, overflow="ellipsis")
    return text


def list_layout(stages: Iterable[Stage], graph: "nx.DiGraph" = None) -> None:
    """ Displays stages in list layout using rich """

    LAYOUT_WIDTH = 80
    LEFT_PAD = (0, 0, 0, 4)

    @render_group()
    def render_stage(stage: "Stage", idx: int):
        """Yields renderables for a single stage."""
        if idx:
            # separator
            yield Rule(style=DVC_LIGHT_ORANGE)
            yield ""

        # title
        title_table = Table.grid(padding=(0, 1), expand=True)
        title = prepare_stage_name(stage, link=True)

        # basic info at the right side of the table
        info_text = Text()
        info = get_info(stage)
        for fragment in prepare_info(info):
            info_text.append(fragment)
        title_table.add_row(title, info_text)

        # pushing all columns except the first one with title, to the right
        for idx, column in enumerate(title_table.columns):
            if not idx:
                continue
            column.no_wrap = True
            column.justify = "right"
        yield title_table
        yield ""

        # Table of ancestor nodes
        nodes_table = Table.grid(padding=(0, 1), expand=True)
        nodes_text = prepare_graph_text(stage, graph)
        if nodes_text:
            nodes_table.add_row(Padding(nodes_text, LEFT_PAD))
            yield nodes_table
            yield ""

        desc = prepare_description(stage)
        yield Padding(desc, LEFT_PAD)

    def column(renderable):
        """Constrain width and align to center to create a column."""
        return Align.center(renderable, width=LAYOUT_WIDTH, pad=False)

    console = Console()
    for idx, stage in enumerate(stages):
        console.print(column(render_stage(stage, idx)))


class CmdStages(CmdBase):
    def _display_short(self, stages: List["Stage"]):
        if self.args.short:
            for stage in stages:
                print(stage.addressing)
            return 0

    def run(self):
        def log_error(relpath: str, exc: Exception):
            if self.args.fail:
                raise exc
            logger.debug("Stages from %s failed to load", relpath)

        # silence stage collection error by default
        self.repo.stage_collection_error_handler = log_error
        if self.args.all:
            stages = self.repo.stages
            logger.trace("%d no. of stages found", len(stages))
        else:
            # removing duplicates while keeping order
            stages = list(
                dict.fromkeys(
                    cat(
                        self.repo.stage.collect(
                            target=target,
                            recursive=self.args.recursive,
                            accept_group=True,
                        )
                        for target in self.args.targets
                    )
                )
            )

        if self.args.short:
            return self._display_short(stages)

        # if graph checks fail, skip showing downstream nodes
        try:
            if self.args.all:
                graph = self.repo.graph
            else:
                # use partial graph by default, so as not to make it too slow
                trie = build_outs_trie(stages)
                graph = build_graph(stages, trie)
        except DvcException:
            if self.args.fail:
                raise
            graph = None
            logger.debug("failed to load graph")

        list_layout(stages, graph)
        return 0


def add_parser(subparsers, parent_parser):
    stages_parser = subparsers.add_parser(
        "stages", parents=[parent_parser], help="List stages",
    )
    stages_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Load all stages inside the directory",
    )
    stages_parser.add_argument(
        "--fail",
        action="store_true",
        default=False,
        help="Fail immediately if there's any error",
    )
    stages_parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help=("List all of the stages present in the repo"),
    )
    stages_parser.add_argument(
        "targets",
        nargs="*",
        default=[PIPELINE_FILE],
        help="Stages to list from",
    ).complete = completion.STAGE
    stages_parser.add_argument(
        "-s",
        "--short",
        action="store_true",
        default=False,
        help="List only the names of the stage",
    )
    stages_parser.set_defaults(func=CmdStages)
