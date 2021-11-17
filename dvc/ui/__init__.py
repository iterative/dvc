from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    TextIO,
    Type,
    Union,
)

from funcy import cached_property

if TYPE_CHECKING:
    from rich.status import Status
    from rich.text import Text as RichText

    from dvc.progress import Tqdm
    from dvc.types import StrPath
    from dvc.ui.table import Headers, Styles, TableData


class Formatter:
    def __init__(self, theme: Dict = None, defaults: Dict = None) -> None:
        from collections import defaultdict

        theme = theme or {
            "success": {"color": "green", "style": "bold"},
            "warn": {"color": "yellow"},
            "error": {"color": "red", "style": "bold"},
        }
        self.theme = defaultdict(lambda: defaults or {}, theme)

    def format(self, message: str, style: str = None, **kwargs) -> str:
        from dvc.utils import colorize

        return colorize(message, **self.theme[style])


class Console:
    def __init__(
        self, formatter: Formatter = None, enable: bool = False
    ) -> None:
        from contextvars import ContextVar

        self.formatter: Formatter = formatter or Formatter()
        self._enabled: bool = enable
        self._paginate: ContextVar[bool] = ContextVar(
            "_paginate", default=False
        )

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
        indent: int = None,
        highlight: bool = None,
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

        console = self.error_console if stderr else self.rich_console
        return console.print_json(
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

    def write(
        self,
        *objects: Any,
        style: str = None,
        sep: str = None,
        end: str = None,
        stderr: bool = False,
        force: bool = False,
        styled: bool = False,
        file: TextIO = None,
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
                console = self.error_console if stderr else self.rich_console
                if styled:
                    return console.print(*objects, sep=sep, end=end)
                return console.out(*objects, sep=sep, end=end, highlight=False)

            values = (self.formatter.format(obj, style) for obj in objects)
            return print(*values, sep=sep, end=end, file=file)

    @property
    def rich_text(self) -> "Type[RichText]":
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

        return console.Console()

    @cached_property
    def error_console(self):
        from rich import console

        return console.Console(stderr=True)

    def table(
        self,
        data: "TableData",
        headers: "Headers" = None,
        markdown: bool = False,
        rich_table: bool = False,
        force: bool = True,
        pager: bool = False,
        header_styles: Union[Dict[str, "Styles"], Sequence["Styles"]] = None,
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
            self, data, headers, markdown=markdown, pager=pager, force=force
        )

    def status(self, status: str, **kwargs: Any) -> "Status":
        return self.error_console.status(status, **kwargs)

    def isatty(self) -> bool:
        import sys

        return sys.stdout.isatty()

    def open_browser(self, file: "StrPath") -> int:
        import webbrowser
        from pathlib import Path
        from platform import uname

        from dvc.utils import relpath

        path = Path(file).resolve()
        url = (
            relpath(path) if "Microsoft" in uname().release else path.as_uri()
        )

        opened = webbrowser.open(url)

        if not opened:
            ui.error_write(
                f"Failed to open {url}. " "Please try opening it manually."
            )
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
    ui.table(
        [("scores.json", "0.5674")], headers=["Path", "auc"], markdown=True
    )
