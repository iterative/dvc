import sys
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Optional,
    Sequence,
    TextIO,
    Union,
)

from dvc.progress import Tqdm
from dvc.utils import colorize

if TYPE_CHECKING:
    from dvc.ui.table import Headers, Styles, TableData


class Formatter:
    def __init__(self, theme: Dict = None, defaults: Dict = None) -> None:
        theme = theme or {
            "success": {"color": "green", "style": "bold"},
            "warn": {"color": "yellow"},
            "error": {"color": "red", "style": "bold"},
        }
        self.theme = defaultdict(lambda: defaults or {}, theme)

    def format(self, message: str, style: str = None, **kwargs) -> str:
        return colorize(message, **self.theme[style])


class Console:
    def __init__(
        self,
        formatter: Formatter = None,
        output: TextIO = None,
        error: TextIO = None,
        enable: bool = False,
    ) -> None:
        self._output: Optional[TextIO] = output
        self._error: Optional[TextIO] = error
        self.formatter: Formatter = formatter or Formatter()
        self._enabled: bool = enable

    @property
    def output(self) -> TextIO:
        return self._output or sys.stdout

    @property
    def error_output(self) -> TextIO:
        return self._error or sys.stderr

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
        style: str = None,
        sep: str = None,
        end: str = None,
        flush: bool = False,
    ) -> None:
        return self.write(
            *objects,
            style=style,
            sep=sep,
            end=end,
            file=self.error_output,
            flush=flush,
        )

    def write(
        self,
        *objects: Any,
        style: str = None,
        sep: str = None,
        end: str = None,
        file: TextIO = None,
        flush: bool = False,
        force: bool = False,
    ) -> None:
        if not self._enabled and not force:
            return

        file = file or self.output
        values = (self.formatter.format(obj, style=style) for obj in objects)
        return print(*values, sep=sep, end=end, file=file, flush=flush)

    def progress(self, *args, **kwargs) -> Tqdm:
        kwargs.setdefault("file", self.error_output)
        return Tqdm(*args, **kwargs)

    def prompt(
        self, text: str, choices: Iterable[str] = None, password: bool = False
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

            self.write(
                f"Your response must be one of: {choices}. "
                "Please try again."
            )

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

    @property
    def rich_console(self):
        """rich_console is only set to stdout for now."""
        from rich import console

        # FIXME: Getting IO Operation on closed file error
        #  when testing with capsys, therefore we are creating
        #  one instance each time as a temporary workaround.
        return console.Console(file=self.output)

    def table(
        self,
        data: "TableData",
        headers: "Headers" = None,
        markdown: bool = False,
        rich_table: bool = False,
        force: bool = True,
        pager: bool = False,
        header_styles: Sequence["Styles"] = None,
        row_styles: Sequence["Styles"] = None,
        borders: Union[bool, str] = False,
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
            self, data, headers, markdown=markdown, pager=pager, force=force,
        )


ui = Console()


if __name__ == "__main__":
    ui.enable()

    ui.write("No default remote set")
    ui.success("Everything is up to date.")
    ui.warn("Run queued experiments will be removed.")
    ui.error("too few arguments.")

    ui.table([("scores.json", "0.5674")], headers=["Path", "auc"])
    ui.table(
        [("scores.json", "0.5674")], headers=["Path", "auc"], markdown=True
    )
