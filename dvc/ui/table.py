from collections import abc
from contextlib import ExitStack, contextmanager
from itertools import zip_longest
from typing import TYPE_CHECKING, Dict, Iterator, Optional, Sequence, Union

from dvc.types import DictStrAny

if TYPE_CHECKING:
    from rich.console import Console as RichConsole
    from rich.table import Table

    from dvc.ui import Console, RichText

SHOW_MAX_WIDTH = 1024


CellT = Union[str, "RichText", None]  # RichText is mostly compatible with str
Row = Sequence[CellT]
TableData = Sequence[Row]
Headers = Sequence[str]
Styles = DictStrAny


def plain_table(
    ui: "Console",
    data: TableData,
    headers: Optional[Headers] = None,
    markdown: bool = False,
    pager: bool = False,
    force: bool = True,
) -> None:
    from funcy import nullcontext
    from tabulate import tabulate

    text: str = tabulate(
        data,
        headers if headers is not None else (),
        tablefmt="github" if markdown else "plain",
        disable_numparse=True,
        # None will be shown as "" by default, overriding
        missingval="-",
    )
    if markdown:
        # NOTE: md table is incomplete without the trailing newline
        text += "\n"

    cm = ui.pager() if pager else nullcontext()
    with cm:
        ui.write(text, force=force)


@contextmanager
def console_width(table: "Table", console: "RichConsole", val: int) -> Iterator[None]:
    # NOTE: rich does not have native support for unlimited width
    # via pager. we override rich table compression by setting
    # console width to the full width of the table

    console_options = console.options
    original = console_options.max_width
    con_width = console._width

    try:
        console_options.max_width = val
        measurement = table.__rich_measure__(console, console_options)
        console._width = measurement.maximum

        yield
    finally:
        console_options.max_width = original
        console._width = con_width


def rich_table(
    ui: "Console",
    data: TableData,
    headers: Optional[Headers] = None,
    pager: bool = False,
    header_styles: Optional[Union[Dict[str, Styles], Sequence[Styles]]] = None,
    row_styles: Optional[Sequence[Styles]] = None,
    borders: Union[bool, str] = False,
) -> None:
    from rich import box

    from dvc.utils.table import Table

    border_style = {
        True: box.HEAVY_HEAD,  # is a default in rich,
        False: None,
        "simple": box.SIMPLE,
        "minimal": box.MINIMAL,
        "horizontals": box.HORIZONTALS,
    }

    table = Table(box=border_style[borders])

    if isinstance(header_styles, abc.Sequence):
        hs: Dict[str, Styles] = dict(zip(headers or [], header_styles))
    else:
        hs = header_styles or {}

    for header in headers or []:
        table.add_column(header, **hs.get(header, {}))

    rs: Sequence[Styles] = row_styles or []
    for row, style in zip_longest(data, rs):
        table.add_row(*row, **(style or {}))

    stack = ExitStack()
    if pager:
        stack.enter_context(console_width(table, ui.rich_console, SHOW_MAX_WIDTH))
        stack.enter_context(ui.pager())

    with stack:
        ui.write(table, styled=True)
        return
