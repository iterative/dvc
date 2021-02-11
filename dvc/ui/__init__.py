import sys
from collections import defaultdict
from typing import Any, Dict, Iterable, Optional, TextIO

from funcy import cached_property

from dvc.progress import Tqdm
from dvc.utils import colorize

NEWLINE = "\n"
SEP = " "


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
        disable: bool = False,
    ) -> None:
        self._input: TextIO = sys.stdin
        self._output: TextIO = output or sys.stdout
        self._error: TextIO = error or sys.stderr

        self.formatter: Formatter = formatter or Formatter()
        self.disabled: bool = disable

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
        sep: str = SEP,
        end: str = NEWLINE,
        flush: bool = False,
    ) -> None:
        return self.write(
            *objects,
            style=style,
            sep=sep,
            end=end,
            file=self._error,
            flush=flush,
        )

    def write(
        self,
        *objects: Any,
        style: str = None,
        sep: str = SEP,
        end: str = NEWLINE,
        file: TextIO = None,
        flush: bool = False,
    ) -> None:
        if self.disabled:
            return

        file = file or self._output
        values = (self.formatter.format(obj, style=style) for obj in objects)
        return print(*values, sep=sep, end=end, file=file, flush=flush)

    def progress(self, *args, **kwargs) -> Tqdm:
        kwargs.setdefault("file", self._error)
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

    @cached_property
    def rich_console(self):
        """rich_console is only set to stdout for now."""
        from rich import console

        return console.Console(file=self._output)

    def rich_table(self, pager: bool = True):
        pass

    def table(self, header, rows, markdown: bool = False):
        from tabulate import tabulate

        if not rows and not markdown:
            return ""

        ret = tabulate(
            rows,
            header,
            tablefmt="github" if markdown else "plain",
            disable_numparse=True,
            # None will be shown as "" by default, overriding
            missingval="—",
        )

        if markdown:
            # NOTE: md table is incomplete without the trailing newline
            ret += "\n"

        self.write(ret)


if __name__ == "__main__":
    ui = Console()

    ui.write("No default remote set")
    ui.success("Everything is up to date.")
    ui.warn("Run queued experiments will be removed.")
    ui.error("too few arguments.")

    ui.table("keys", {"Path": ["scores.json"], "auc": ["0.5674"]})
    ui.table(
        "keys", {"Path": ["scores.json"], "auc": ["0.5674"]}, markdown=True
    )
