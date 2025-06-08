from funcy import split
from rich.progress import (
    BarColumn,
    DownloadColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)


class MofNCompleteColumnWithUnit(MofNCompleteColumn):
    """Requires `task.fields[unit]` to be set."""

    def render(self, task):
        ret = super().render(task)
        unit = task.fields.get("unit")
        return ret.append(f" {unit}") if unit else ret


class RichProgress(Progress):
    def clear_task(self, task):
        try:
            self.remove_task(task)
        except KeyError:
            pass


class RichTransferProgress(RichProgress):
    SUMMARY_COLS = (
        TextColumn("[magenta]{task.description}[bold green]"),
        MofNCompleteColumnWithUnit(),
        TimeElapsedColumn(),
    )
    TRANSFER_COLS = (
        TextColumn("  [blue]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TextColumn("eta"),
        TimeRemainingColumn(),
    )

    def get_renderables(self):
        summary_tasks, other_tasks = split(
            lambda task: task.fields.get("progress_type") == "summary",
            self.tasks,
        )
        self.columns = self.SUMMARY_COLS
        yield self.make_tasks_table(summary_tasks)
        self.columns = self.TRANSFER_COLS
        yield self.make_tasks_table(other_tasks)


class DbDownloadProgress(RichProgress):
    PROGRESS_COLS = (
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("[progress.download]{task.completed:,} rows"),
        TextColumn("to [repr.filename]{task.fields[output]}"),
    )
    STATUS_COLS = (
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("to [repr.filename]{task.fields[output]}"),
    )

    def get_renderables(self):
        if self.tasks:
            (task, *_) = self.tasks
            cols = self.PROGRESS_COLS if task.completed else self.STATUS_COLS
            self.columns = cols[1:] if task.finished else cols
        yield self.make_tasks_table(self.tasks)
