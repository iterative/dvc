from collections import abc
from collections.abc import (
    ItemsView,
    Iterable,
    Iterator,
    Mapping,
    MutableSequence,
    Sequence,
)
from itertools import chain, repeat, zip_longest
from operator import itemgetter
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from funcy import reraise

if TYPE_CHECKING:
    from dvc.ui.table import CellT


class Column(list["CellT"]):
    pass


def with_value(value, default):
    return default if value is None else value


class TabularData(MutableSequence[Sequence["CellT"]]):
    def __init__(self, columns: Sequence[str], fill_value: Optional[str] = ""):
        self._columns: dict[str, Column] = {name: Column() for name in columns}
        self._keys: list[str] = list(columns)
        self._fill_value = fill_value
        self._protected: set[str] = set()

    @property
    def columns(self) -> list[Column]:
        return list(map(self.column, self.keys()))

    def is_protected(self, col_name) -> bool:
        return col_name in self._protected

    def protect(self, *col_names: str):
        self._protected.update(col_names)

    def unprotect(self, *col_names: str):
        self._protected = self._protected.difference(col_names)

    def column(self, name: str) -> Column:
        return self._columns[name]

    def items(self) -> ItemsView[str, Column]:
        projection = {k: self.column(k) for k in self.keys()}
        return projection.items()

    def keys(self) -> list[str]:
        return self._keys

    def _iter_col_row(self, row: Sequence["CellT"]) -> Iterator[tuple["CellT", Column]]:
        for val, col in zip_longest(row, self.columns):
            if col is None:
                break
            yield with_value(val, self._fill_value), col

    def append(self, value: Sequence["CellT"]) -> None:
        for val, col in self._iter_col_row(value):
            col.append(val)

    def extend(self, values: Iterable[Sequence["CellT"]]) -> None:
        for row in values:
            self.append(row)

    def insert(self, index: int, value: Sequence["CellT"]) -> None:
        for val, col in self._iter_col_row(value):
            col.insert(index, val)

    def __iter__(self) -> Iterator[list["CellT"]]:
        return map(list, zip(*self.columns))

    def __getattr__(self, item: str) -> Column:
        with reraise(KeyError, AttributeError):
            return self.column(item)

    def __getitem__(self, item: Union[int, slice]):
        func = itemgetter(item)
        it = map(func, self.columns)
        if isinstance(item, slice):
            it = map(list, zip(*it))
        return list(it)

    @overload
    def __setitem__(self, item: int, value: Sequence["CellT"]) -> None: ...

    @overload
    def __setitem__(self, item: slice, value: Iterable[Sequence["CellT"]]) -> None: ...

    def __setitem__(self, item, value) -> None:
        it = value
        if isinstance(item, slice):
            n = len(self.columns)
            normalized_rows = (
                chain(val, repeat(self._fill_value, n - len(val))) for val in value
            )
            # we need to transpose those rows into columnar format
            # as we work in terms of column-based arrays
            it = zip(*normalized_rows)

        for i, col in self._iter_col_row(it):
            col[item] = i

    def __delitem__(self, item: Union[int, slice]) -> None:
        for col in self.columns:
            del col[item]

    def __len__(self) -> int:
        if not self._columns:
            return 0
        return len(self.columns[0])

    @property
    def shape(self) -> tuple[int, int]:
        return len(self.columns), len(self)

    def drop(self, *col_names: str) -> None:
        for col in col_names:
            if not self.is_protected(col):
                self._keys.remove(col)
                self._columns.pop(col)

    def rename(self, from_col_name: str, to_col_name: str) -> None:
        self._columns[to_col_name] = self._columns.pop(from_col_name)
        self._keys[self._keys.index(from_col_name)] = to_col_name

    def project(self, *col_names: str) -> None:
        self.drop(*(set(self._keys) - set(col_names)))
        self._keys = list(col_names)

    def is_empty(self, col_name: str) -> bool:
        col = self.column(col_name)
        return not any(item != self._fill_value for item in col)

    def to_csv(self) -> str:
        import csv
        from io import StringIO

        buff = StringIO()
        writer = csv.writer(buff)
        writer.writerow(self.keys())

        for row in self:
            writer.writerow(row)
        return buff.getvalue()

    def add_column(self, name: str) -> None:
        self._columns[name] = Column([self._fill_value] * len(self))
        self._keys.append(name)

    def row_from_dict(self, d: Mapping[str, "CellT"]) -> None:
        keys = self.keys()
        for key in d:
            if key not in keys:
                self.add_column(key)

        row: list[CellT] = [
            with_value(d.get(key), self._fill_value) for key in self.keys()
        ]
        self.append(row)

    def render(self, **kwargs: Any):
        from dvc.ui import ui

        if kwargs.pop("csv", False):
            ui.write(self.to_csv(), end="")
        else:
            ui.table(self, headers=self.keys(), **kwargs)

    def as_dict(
        self, cols: Optional[Iterable[str]] = None
    ) -> Iterable[dict[str, "CellT"]]:
        keys = self.keys() if cols is None else set(cols)
        return [{k: self._columns[k][i] for k in keys} for i in range(len(self))]

    def dropna(  # noqa: C901, PLR0912
        self,
        axis: str = "rows",
        how="any",
        subset: Optional[Iterable[str]] = None,
    ):
        if axis not in ["rows", "cols"]:
            raise ValueError(
                f"Invalid 'axis' value {axis}.Choose one of ['rows', 'cols']"
            )
        if how not in ["any", "all"]:
            raise ValueError(f"Invalid 'how' value {how}. Choose one of ['any', 'all']")

        match_line: set = set()
        match_any = True
        if how == "all":
            match_any = False

        for n_row, row in enumerate(self):
            for n_col, col in enumerate(row):
                if subset and self.keys()[n_col] not in subset:
                    continue
                if (col == self._fill_value) is match_any:
                    if axis == "rows":
                        match_line.add(n_row)
                        break
                    match_line.add(self.keys()[n_col])

        to_drop = match_line
        if how == "all":
            if axis == "rows":
                to_drop = set(range(len(self)))
            else:
                to_drop = set(self.keys())
            to_drop -= match_line

        if axis == "rows":
            for name in self.keys():
                self._columns[name] = Column(
                    [x for n, x in enumerate(self._columns[name]) if n not in to_drop]
                )
        else:
            self.drop(*to_drop)

    def drop_duplicates(  # noqa: C901
        self,
        axis: str = "rows",
        subset: Optional[Iterable[str]] = None,
        ignore_empty: bool = True,
    ):
        if axis not in ["rows", "cols"]:
            raise ValueError(
                f"Invalid 'axis' value {axis}.Choose one of ['rows', 'cols']"
            )

        if axis == "cols":
            cols_to_drop: list[str] = []
            for n_col, col in enumerate(self.columns):
                if subset and self.keys()[n_col] not in subset:
                    continue
                # Cast to str because Text is not hashable error
                unique_vals = {str(x) for x in col}
                if ignore_empty and self._fill_value in unique_vals:
                    unique_vals -= {self._fill_value}
                if len(unique_vals) == 1:
                    cols_to_drop.append(self.keys()[n_col])
            self.drop(*cols_to_drop)

        elif axis == "rows":
            unique_rows = []
            rows_to_drop: list[int] = []
            for n_row, row in enumerate(self):
                if subset:
                    row = [
                        col
                        for n_col, col in enumerate(row)
                        if self.keys()[n_col] in subset
                    ]

                tuple_row = tuple(row)
                if tuple_row in unique_rows:
                    rows_to_drop.append(n_row)
                else:
                    unique_rows.append(tuple_row)

            for name in self.keys():
                self._columns[name] = Column(
                    [
                        x
                        for n, x in enumerate(self._columns[name])
                        if n not in rows_to_drop
                    ]
                )


def _normalize_float(val: float, precision: int):
    return f"{val:.{precision}g}"


def _format_field(
    val: Any, precision: Optional[int] = None, round_digits: bool = False
) -> str:
    def _format(_val):
        if isinstance(_val, float) and precision:
            if round_digits:
                return round(_val, precision)
            return _normalize_float(_val, precision)
        if isinstance(_val, abc.Mapping):
            return {k: _format(v) for k, v in _val.items()}
        if isinstance(_val, list):
            return [_format(x) for x in _val]
        return _val

    return str(_format(val))


def diff_table(
    diff,
    title: str,
    old: bool = True,
    no_path: bool = False,
    show_changes: bool = True,
    precision: Optional[int] = None,
    round_digits: bool = False,
    on_empty_diff: Optional[str] = None,
    a_rev: Optional[str] = None,
    b_rev: Optional[str] = None,
) -> TabularData:
    a_rev = a_rev or "HEAD"
    b_rev = b_rev or "workspace"
    headers: list[str] = ["Path", title, a_rev, b_rev, "Change"]
    fill_value = "-"
    td = TabularData(headers, fill_value=fill_value)

    for fname, diff_in_file in diff.items():
        for item, change in sorted(diff_in_file.items()):
            old_value = with_value(change.get("old"), fill_value)
            new_value = with_value(change.get("new"), fill_value)
            diff_value = with_value(change.get("diff", on_empty_diff), fill_value)
            td.append(
                [
                    fname,
                    str(item),
                    _format_field(old_value, precision, round_digits),
                    _format_field(new_value, precision, round_digits),
                    _format_field(diff_value, precision, round_digits),
                ]
            )

    if no_path:
        td.drop("Path")

    if not show_changes:
        td.drop("Change")

    if not old:
        td.drop(a_rev)
        td.rename(b_rev, "Value")

    return td


def show_diff(  # noqa: PLR0913
    diff,
    title: str,
    old: bool = True,
    no_path: bool = False,
    show_changes: bool = True,
    precision: Optional[int] = None,
    round_digits: bool = False,
    on_empty_diff: Optional[str] = None,
    markdown: bool = False,
    a_rev: Optional[str] = None,
    b_rev: Optional[str] = None,
) -> None:
    td = diff_table(
        diff,
        title=title,
        old=old,
        no_path=no_path,
        show_changes=show_changes,
        precision=precision,
        round_digits=round_digits,
        on_empty_diff=on_empty_diff,
        a_rev=a_rev,
        b_rev=b_rev,
    )
    td.render(markdown=markdown)


def metrics_table(
    metrics,
    all_branches: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    precision: Optional[int] = None,
    round_digits: bool = False,
):
    from dvc.utils.diff import format_dict
    from dvc.utils.flatten import flatten

    td = TabularData(["Revision", "Path"], fill_value="-")

    for branch, val in metrics.items():
        for fname, metric in val.get("data", {}).items():
            row_data: dict[str, str] = {"Revision": branch, "Path": fname}
            metric = metric.get("data", {})
            flattened = (
                flatten(format_dict(metric))
                if isinstance(metric, dict)
                else {"": metric}
            )
            row_data.update(
                {
                    k: _format_field(v, precision, round_digits)
                    for k, v in flattened.items()
                }
            )
            td.row_from_dict(row_data)

    rev, path, *metrics_headers = td.keys()
    td.project(rev, path, *sorted(metrics_headers))

    if not any([all_branches, all_tags, all_commits]):
        td.drop("Revision")

    return td


def show_metrics(
    metrics,
    markdown: bool = False,
    all_branches: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    precision: Optional[int] = None,
    round_digits: bool = False,
) -> None:
    td = metrics_table(
        metrics,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        precision=precision,
        round_digits=round_digits,
    )
    td.render(markdown=markdown)
