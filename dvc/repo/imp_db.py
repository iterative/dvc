from typing import TYPE_CHECKING, Optional

from funcy import compact

from dvc.exceptions import OutputDuplicationError
from dvc.repo.scm_context import scm_context
from dvc.ui import ui
from dvc.utils import resolve_output, resolve_paths

if TYPE_CHECKING:
    from . import Repo

from . import locked


@locked
@scm_context
def imp_db(
    self: "Repo",
    sql: Optional[str] = None,
    table: Optional[str] = None,
    frozen: bool = True,
    output_format: str = "csv",
    out: Optional[str] = None,
    force: bool = False,
    connection: Optional[str] = None,
):
    ui.warn("WARNING: import-db is an experimental feature.")
    ui.warn(
        "The functionality may change or break without notice, "
        "which could lead to unexpected behavior."
    )
    assert sql or table
    assert output_format in ("csv", "json")

    db: dict[str, str] = compact(
        {
            "connection": connection,
            "file_format": output_format,
            "query": sql,
            "table": table,
        }
    )

    file_name = table or "results"
    out = out or f"{file_name}.{output_format}"
    out = resolve_output(".", out, force=force)

    path, wdir, out = resolve_paths(self, out, always_local=True)
    stage = self.stage.create(
        single_stage=True,
        validate=False,
        fname=path,
        deps=[None],
        wdir=wdir,
        outs=[out],
        db=db,
    )

    try:
        self.check_graph(stages={stage})
    except OutputDuplicationError as exc:
        raise OutputDuplicationError(  # noqa: B904
            exc.output, set(exc.stages) - {stage}
        )

    stage.run()
    stage.frozen = frozen
    stage.dump()
    return stage
