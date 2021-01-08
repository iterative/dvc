import logging
from typing import TYPE_CHECKING, Iterable, List, Tuple, Union

from dvc.command import completion
from dvc.command.base import CmdBase

if TYPE_CHECKING:
    import networkx as nx
    from rich.markdown import Markdown
    from rich.text import Text

    from dvc.stage import Stage


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
    info = {
        "dep": len(stage.deps),
        "out": len(stage.outs) - num_metrics - num_plots,
        "metric": num_metrics,
        "plot": num_plots,
    }
    return [
        (pluralize(key, value), value) for key, value in info.items() if value
    ]


def sizeof_fmt(size: int, suffix: str = "B", decimal_places: int = 1) -> str:
    unit = ""
    for unit in ["", "Ki", "Mi", "Gi"]:
        if size < 1024.0 or unit == "GiB":
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}{suffix}"


def generate_description(stage: "Stage", with_size=True) -> str:
    def part_desc(outs, show_size=with_size) -> str:
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


def prepare_info(info: List[Tuple[str, int]]) -> "Text":
    from rich.text import Text

    ret = Text()
    for index, (key, value) in enumerate(info):
        if index:
            ret.append(PIPE)
        ret.append_text(Text.styled(str(value), style="blue"))
        ret.append_text(Text.styled(f" {key}", style="dim blue"))
    return ret


def prepare_graph_text(stage: "Stage", graph: "nx.DiGraph" = None) -> "Text":
    def gen_name(up):
        # only displaying the name if they are both in the same file
        # though this might be confusing with the stages in the current
        # directory as they don't display the path as well.
        # went to this logic for simplicity and aesthetics.
        use_name = up.path == stage.path
        return prepare_stage_name(up, use_name=use_name)

    from rich.text import Text

    text = Text()
    fork = Text(FORK, style="bold blue")
    # show only the direct dependents of the `stage`
    predecessors = graph.predecessors(stage) if graph else []
    for index, display_name in enumerate(map(gen_name, predecessors)):
        if index:
            text.append(", ")
        text.append_text(fork)
        text.append(display_name)
    text.truncate(MAX_TEXT_LENGTH, overflow="ellipsis")
    return text


def prepare_stage_name(stage: "Stage", link=False, use_name=False) -> "Text":
    import os

    from rich.style import Style
    from rich.text import Text

    from dvc.stage import PipelineStage

    title = Text(overflow="fold")
    address = stage.addressing

    linked = Style(link="file://" + os.path.abspath(stage.relpath))
    file_style = Style(color=DVC_BLUE, bold=True, underline=False)
    name_style = None
    if link:
        # we show a link to the dvc.yaml file in the terminal for the stages
        # need to hover over their name
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


def prepare_description(stage: "Stage") -> Union["Markdown", "Text"]:
    from rich.markdown import Markdown
    from rich.text import Text

    style = "blue"
    if stage.desc:
        # not sure if this was ever intended, but markdown looks nice
        return Markdown(stage.desc, style=style)

    description = generate_description(stage)
    text = Text(description.strip(), style=style)
    text.truncate(MAX_TEXT_LENGTH, overflow="ellipsis")
    return text


def list_layout(stages: Iterable["Stage"], graph: "nx.DiGraph" = None) -> None:
    """ Displays stages in list layout using rich """

    LAYOUT_WIDTH = 80
    LEFT_PAD = (0, 0, 0, 4)
    SHORT_LEFT_PAD = (0, 0, 0, 2)

    def render_stage(stage: "Stage", idx: int):
        """Yields renderables for a single stage."""
        from rich.padding import Padding
        from rich.rule import Rule
        from rich.table import Table

        if idx:
            # separator
            yield Padding(Rule(style=DVC_LIGHT_ORANGE), SHORT_LEFT_PAD)
            yield ""

        # title
        title_table = Table.grid(padding=(0, 1), expand=True)
        title = Padding(prepare_stage_name(stage, link=True), SHORT_LEFT_PAD)

        # basic info at the right side of the table
        info = get_info(stage)
        info_text = prepare_info(info)
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
        """Constrain width and align column to the left."""
        from rich.align import Align

        return Align.left(renderable, width=LAYOUT_WIDTH, pad=False)

    from rich.console import Console, render_group

    console = Console()
    for idx, stage in enumerate(stages):
        renderable = render_group()(render_stage)
        console.print(column(renderable(stage, idx)))


def short_output(stage: "Stage") -> Tuple[str, str]:
    desc = stage.desc or generate_description(stage, with_size=False)
    return stage.addressing, desc


def get_stages_list(stages: List["Stage"]) -> List[Tuple[str, str]]:
    ret: List[Tuple[str, str]] = []
    for stage in stages:
        ret.append(short_output(stage))
    return ret


class CmdStages(CmdBase):
    def _display_short(
        self,
        stages: List["Stage"],
        compact=False,
        names_only=False,
        table=False,
    ):
        if names_only:
            # NOTE: this output format is used for autocompletion as well
            for stage in stages:
                print(stage.addressing)
            return

        data = get_stages_list(stages)
        if compact:
            # NOTE: compact format is used in zsh autocompletion
            for name, desc in data:
                print(f"{name}:'{desc}'")
            return

        from rich.console import Console
        from rich.table import Table

        table = Table() if table else Table.grid((0, 1))
        table.add_column("Stages")
        table.add_column("Description")
        console = Console()
        for name, desc in data:
            table.add_row(name, desc)
        console.print(table)

    def run(self):
        from funcy import cat

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

        if (
            self.args.short
            or self.args.list_names
            or self.args.compact
            or self.args.table
        ):
            self._display_short(
                stages,
                compact=self.args.compact,
                names_only=self.args.list_names,
                table=self.args.table,
            )
            return 0

        from dvc.exceptions import DvcException

        # if graph checks fail, skip showing downstream nodes
        try:
            if self.args.all:
                graph = self.repo.graph
            else:
                from dvc.repo.graph import build_graph
                from dvc.repo.trie import build_outs_trie

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
        "stages", parents=[parent_parser], description="List stages.",
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
        "targets", nargs="*", default=["dvc.yaml"], help="Stages to list from",
    ).complete = completion.STAGE
    stages_parser.add_argument(
        "-s",
        "--short",
        action="store_true",
        default=False,
        help="Prints short output",
    )
    stages_parser.add_argument(
        "--list-names",
        action="store_true",
        default=False,
        help="List only the names of the stages",
    )
    stages_parser.add_argument(
        "--table",
        action="store_true",
        default=False,
        help="Display as a table",
    )
    stages_parser.add_argument(
        "--compact",
        action="store_true",
        default=False,
        help="Show a compact list of names and description",
    )
    stages_parser.set_defaults(func=CmdStages)
