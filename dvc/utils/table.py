from typing import TYPE_CHECKING, Any

from rich.table import Table as RichTable

if TYPE_CHECKING:
    from rich.console import Console, ConsoleOptions


class Table(RichTable):
    def add_column(self, *args: Any, collapse: bool = False, **kwargs: Any) -> None:
        super().add_column(*args, **kwargs)
        self.columns[-1].collapse = collapse  # type: ignore[attr-defined]

    def _calculate_column_widths(
        self, console: "Console", options: "ConsoleOptions"
    ) -> list[int]:
        """Calculate the widths of each column, including padding, not
        including borders.

        Adjacent collapsed columns will be removed until there is only a single
        truncated column remaining.
        """
        widths = super()._calculate_column_widths(console, options)
        last_collapsed = -1
        columns = self.columns
        for i in range(len(columns) - 1, -1, -1):
            if widths[i] == 0 and columns[i].collapse:  # type: ignore[attr-defined]
                if last_collapsed >= 0:
                    del widths[last_collapsed]
                    del columns[last_collapsed]
                    if self.box:
                        options.max_width += 1
                    for column in columns[last_collapsed:]:
                        column._index -= 1
                last_collapsed = i
                padding = self._get_padding_width(i)
                if (
                    columns[i].overflow == "ellipsis"
                    and (sum(widths) + padding) <= options.max_width
                ):
                    # Set content width to 1 (plus padding) if we can fit a
                    # single unicode ellipsis in this column
                    widths[i] = 1 + padding
            else:
                last_collapsed = -1
        return widths

    def _collapse_widths(  # type: ignore[override]
        self,
        widths: list[int],
        wrapable: list[bool],
        max_width: int,
    ) -> list[int]:
        """Collapse columns right-to-left if possible to fit table into
        max_width.

        If table is still too wide after collapsing, rich's automatic overflow
        handling will be used.
        """
        collapsible = [column.collapse for column in self.columns]  # type: ignore[attr-defined]
        total_width = sum(widths)
        excess_width = total_width - max_width
        if any(collapsible):
            for i in range(len(widths) - 1, -1, -1):
                if collapsible[i]:
                    excess_width -= widths[i]
                    widths[i] = 0
                    if excess_width <= 0:
                        break
        return super()._collapse_widths(widths, wrapable, max_width)
