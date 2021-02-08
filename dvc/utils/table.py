from dataclasses import dataclass
from typing import TYPE_CHECKING, List

from rich.style import StyleType
from rich.table import Column as RichColumn
from rich.table import Table as RichTable

if TYPE_CHECKING:
    from rich.console import (
        Console,
        JustifyMethod,
        OverflowMethod,
        RenderableType,
    )


@dataclass
class Column(RichColumn):
    collapse: bool = False


class Table(RichTable):
    def add_column(  # pylint: disable=arguments-differ
        self,
        header: "RenderableType" = "",
        footer: "RenderableType" = "",
        *,
        header_style: StyleType = None,
        footer_style: StyleType = None,
        style: StyleType = None,
        justify: "JustifyMethod" = "left",
        overflow: "OverflowMethod" = "ellipsis",
        width: int = None,
        min_width: int = None,
        max_width: int = None,
        ratio: int = None,
        no_wrap: bool = False,
        collapse: bool = False,
    ) -> None:
        column = Column(  # type: ignore[call-arg]
            _index=len(self.columns),
            header=header,
            footer=footer,
            header_style=header_style or "",
            footer_style=footer_style or "",
            style=style or "",
            justify=justify,
            overflow=overflow,
            width=width,
            min_width=min_width,
            max_width=max_width,
            ratio=ratio,
            no_wrap=no_wrap,
            collapse=collapse,
        )
        self.columns.append(column)

    def _calculate_column_widths(
        self, console: "Console", max_width: int
    ) -> List[int]:
        """Calculate the widths of each column, including padding, not
        including borders.

        Adjacent collapsed columns will be removed until there is only a single
        truncated column remaining.
        """
        widths = super()._calculate_column_widths(console, max_width)
        last_collapsed = -1
        for i in range(len(self.columns) - 1, -1, -1):
            if widths[i] == 1 and self.columns[i].collapse:
                if last_collapsed >= 0:
                    del widths[last_collapsed]
                    del self.columns[last_collapsed]
                    if self.box:
                        max_width += 1
                    for column in self.columns[last_collapsed:]:
                        column._index -= 1
                last_collapsed = i
                padding = self._get_padding_width(i)
                if (
                    self.columns[i].overflow == "ellipsis"
                    and (sum(widths) + padding) <= max_width
                ):
                    # Set content width to 1 (plus padding) if we can fit a
                    # single unicode ellipsis in this column
                    widths[i] = 1 + padding
            else:
                last_collapsed = -1
        return widths

    def _collapse_widths(
        self, widths: List[int], wrapable: List[bool], max_width: int,
    ) -> List[int]:
        """Collapse columns right-to-left if possible to fit table into
        max_width.

        If table is still too wide after collapsing, rich's automatic overflow
        handling will be used.
        """
        collapsible = [column.collapse for column in self.columns]
        total_width = sum(widths)
        excess_width = total_width - max_width
        if any(collapsible):
            for i in range(len(widths) - 1, -1, -1):
                if collapsible[i]:
                    total_width -= widths[i]
                    excess_width -= widths[i]
                    widths[i] = 0
                    if excess_width <= 0:
                        break
        return super()._collapse_widths(widths, wrapable, max_width)
