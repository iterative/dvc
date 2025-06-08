from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager, nullcontext
from typing import TYPE_CHECKING, Any, Callable, Optional, TextIO, Union

import colorama

from dvc.utils.objects import cached_property

if TYPE_CHECKING:
    from rich.console import Console as RichConsole
    from rich.console import JustifyMethod, OverflowMethod
    from rich.status import Status
    from rich.style import Style
    from rich.text import Text as RichText

    from dvc.progress import Tqdm
    from dvc.types import StrPath
    from dvc.ui.table import Headers, Styles, TableData


@contextmanager
def disable_colorama():
    import sys

    colorama.deinit()
    try:
        yield
    finally:
        if sys.stdout:
            sys.stdout.flush()
        if sys.stderr:
            sys.stderr.flush()
        colorama.reinit()


class Formatter:
    def __init__(
        self, theme: Optional[dict] = None, defaults: Optional[dict] = None
    ) -> None:
        from collections import defaultdict

        theme = theme or {
            "success": {"color": "green", "style": "bold"},
            "warn": {"color": "yellow"},
            "error": {"color": "red", "style": "bold"},
        }
        self.theme = defaultdict(lambda: defaults or {}, theme)

    def format(self, message: str, style: Optional[str] = None, **kwargs) -> str:
        from dvc.utils import colorize

        return colorize(message, **self.theme[style])


class Console:
    def __init__(
        self, formatter: Optional[Formatter] = None, enable: bool = False
    ) -> None:
        from contextvars import ContextVar

        self.formatter: Formatter = formatter or Formatter()
        self._enabled: bool = enable
        self._paginate: ContextVar[bool] = ContextVar("_paginate", default=False)

    def enable(self) -> None:
        self._enabled = True

    def success(self, message: str) -> None:
        self.write(message, style="success")

    def error(self, message: str) -> None:
        self.error_write(message, style="error")

    def warn(self, message: str) -> None:
        self.error_write(message, style="warn")

    def error_write(
        self,
        *objects: Any,
        style: Optional[str] = None,
        sep: Optional[str] = None,
        end: Optional[str] = None,
        styled: bool = False,
        force: bool = True,
    ) -> None:
        return self.write(
            *objects,
            style=style,
            sep=sep,
            end=end,
            stderr=True,
            force=force,
            styled=styled,
        )

    def write_json(
        self,
        data: Any,
        indent: Optional[int] = None,
        highlight: Optional[bool] = None,
        stderr: bool = False,
        skip_keys: bool = False,
        ensure_ascii: bool = True,
        check_circular: bool = True,
        allow_nan: bool = True,
        default: Optional[Callable[[Any], Any]] = None,
        sort_keys: bool = False,
    ) -> None:
        if highlight is None:
            highlight = self.isatty()
        if indent is None and self.isatty():
            indent = 2

        from rich.json import JSON

        json = JSON.from_data(
            data=data,
            indent=indent,
            highlight=bool(highlight),
            skip_keys=skip_keys,
            ensure_ascii=ensure_ascii,
            check_circular=check_circular,
            allow_nan=allow_nan,
            default=default,
            sort_keys=sort_keys,
        )
        if not highlight:
            import os

            # we don't need colorama to try to strip ansi codes
            # when highlighting is disabled
            ctx = nullcontext() if "DVC_TEST" in os.environ else disable_colorama()
            with ctx:
                return self.write(json.text, stderr=stderr)
        return self.rich_print(json, stderr=stderr, soft_wrap=True)

    def rich_print(  # noqa: PLR0913
        self,
        *objects: Any,
        sep: str = " ",
        end: str = "\n",
        stderr: bool = False,
        style: Optional[Union[str, "Style"]] = None,
        justify: Optional["JustifyMethod"] = None,
        overflow: Optional["OverflowMethod"] = None,
        no_wrap: Optional[bool] = None,
        emoji: Optional[bool] = None,
        markup: Optional[bool] = None,
        highlight: Optional[bool] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
        crop: bool = True,
        soft_wrap: Optional[bool] = None,
        new_line_start: bool = False,
    ) -> None:
        if stderr:
            console = self.error_console
        else:
            console = self.rich_console
        return console.print(
            *objects,
            sep=sep,
            end=end,
            style=style,
            justify=justify,
            overflow=overflow,
            no_wrap=no_wrap,
            emoji=emoji,
            markup=markup,
            highlight=highlight,
            width=width,
            height=height,
            crop=crop,
            soft_wrap=soft_wrap,
            new_line_start=new_line_start,
        )

    def write(
        self,
        *objects: Any,
        style: Optional[str] = None,
        sep: Optional[str] = None,
        end: Optional[str] = None,
        stderr: bool = False,
        force: bool = False,
        styled: bool = False,
        file: Optional[TextIO] = None,
    ) -> None:
        import sys

        from dvc.progress import Tqdm

        sep = " " if sep is None else sep
        end = "\n" if end is None else end
        if not self._enabled and not force:
            return

        file = file or (sys.stderr if stderr else sys.stdout)
        with Tqdm.external_write_mode(file=file):
            # if we are inside pager context, send the output to rich's buffer
            if styled or self._paginate.get():
                if styled:
                    return self.rich_print(*objects, sep=sep, end=end, stderr=stderr)
                return self.rich_print(
                    sep.join(str(_object) for _object in objects),
                    style=None,
                    highlight=False,
                    emoji=False,
                    markup=False,
                    no_wrap=True,
                    overflow="ignore",
                    crop=False,
                    sep=sep,
                    end=end,
                    stderr=stderr,
                )

            values = (self.formatter.format(obj, style) for obj in objects)
            return print(*values, sep=sep, end=end, file=file)

    @property
    def rich_text(self) -> "type[RichText]":
        from rich.text import Text

        return Text

    @staticmethod
    def progress(*args, **kwargs) -> "Tqdm":
        from dvc.progress import Tqdm

        return Tqdm(*args, **kwargs)

    @contextmanager
    def pager(self, styles: bool = True) -> Iterator[None]:
        from .pager import DvcPager

        tok = self._paginate.set(True)
        try:
            with self.rich_console.pager(pager=DvcPager(), styles=styles):
                yield
        finally:
            self._paginate.reset(tok)

    def prompt(
        self,
        text: str,
        choices: Optional[Iterable[str]] = None,
        password: bool = False,
    ) -> Optional[str]:
        while True:
            try:
                response = self.rich_console.input(
                    text + " ", markup=False, password=password
                )
            except EOFError:
                return None

            answer = response.lower()
            if not choices:
                return answer

            if answer in choices:
                return answer

            self.write(f"Your response must be one of: {choices}. Please try again.")

    def confirm(self, statement: str) -> bool:
        """Ask the user for confirmation about the specified statement.

        Args:
            statement: statement to ask the user confirmation about.
        """
        text = f"{statement} [y/n]:"
        answer = self.prompt(text, choices=["yes", "no", "y", "n"])
        if not answer:
            return False
        return answer.startswith("y")

    @cached_property
    def rich_console(self) -> "RichConsole":
        """rich_console is only set to stdout for now."""
        from rich import console

        return console.Console()

    @cached_property
    def error_console(self) -> "RichConsole":
        from rich import console

        return console.Console(stderr=True)

    def table(
        self,
        data: "TableData",
        headers: Optional["Headers"] = None,
        markdown: bool = False,
        rich_table: bool = False,
        force: bool = True,
        pager: bool = False,
        header_styles: Optional[Union[dict[str, "Styles"], Sequence["Styles"]]] = None,
        row_styles: Optional[Sequence["Styles"]] = None,
        borders: Union[bool, str] = False,
        colalign: Optional[tuple[str, ...]] = None,
    ) -> None:
        from dvc.ui import table as t

        if not data and not markdown:
            return

        if not markdown and rich_table:
            if force or self._enabled:
                return t.rich_table(
                    self,
                    data,
                    headers,
                    pager=pager,
                    header_styles=header_styles,
                    row_styles=row_styles,
                    borders=borders,
                )

            return

        return t.plain_table(
            self,
            data,
            headers,
            markdown=markdown,
            pager=pager,
            force=force,
            colalign=colalign,
        )

    def status(self, status: str, **kwargs: Any) -> "Status":
        return self.error_console.status(status, **kwargs)

    @staticmethod
    def isatty() -> bool:
        import sys

        from dvc import utils

        return utils.isatty(sys.stdout)

    def open_browser(self, file: "StrPath") -> int:
        import webbrowser
        from pathlib import Path
        from platform import uname

        from dvc.utils import relpath

        path = Path(file).resolve()
        url = relpath(path) if "microsoft" in uname().release.lower() else path.as_uri()

        opened = webbrowser.open(url)

        if not opened:
            ui.error_write(f"Failed to open {url}. Please try opening it manually.")
            return 1

        return 0


ui = Console()


if __name__ == "__main__":
    ui.enable()

    ui.write("No default remote set")
    ui.success("Everything is up to date.")
    ui.warn("Run queued experiments will be removed.")
    ui.error("too few arguments.")

    ui.table([("scores.json", "0.5674")], headers=["Path", "auc"])
    ui.table([("scores.json", "0.5674")], headers=["Path", "auc"], markdown=True)
