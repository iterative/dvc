from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    import sqlalchemy as sa
    from agate import Table


def noop(_):
    pass


@dataclass
class PandasSQLSerializer:
    sql: "Union[sa.TextClause, str]"
    con: "sa.Connection"
    chunksize: int = 10_000

    def to_csv(self, file: str, progress=noop) -> None:
        import pandas as pd

        with open(file, mode="wb") as f:
            idfs = pd.read_sql_query(self.sql, self.con, chunksize=self.chunksize)
            for i, df in enumerate(idfs):
                df.to_csv(f, header=i == 0, index=False)
                progress(len(df))

    def to_json(self, file: str, progress=noop) -> None:  # noqa: ARG002
        import pandas as pd

        df = pd.read_sql_query(self.sql, self.con)
        df.to_json(file, orient="records")


@dataclass
class AgateSerializer:
    table: "Table"

    def to_csv(self, file: str, progress=noop) -> None:  # noqa: ARG002
        return self.table.to_csv(file)

    def to_json(self, file: str, progress=noop) -> None:  # noqa: ARG002
        return self.table.to_json(file)


def export(
    serializer: Union[PandasSQLSerializer, AgateSerializer],
    file: str,
    format: str = "csv",  # noqa: A002
    progress=noop,
) -> None:
    if format == "csv":
        return serializer.to_csv(file, progress=progress)
    return serializer.to_json(file, progress=progress)
