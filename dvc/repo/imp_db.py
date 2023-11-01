from typing import TYPE_CHECKING, Optional

from dvc.exceptions import OutputDuplicationError
from dvc.repo.scm_context import scm_context
from dvc.utils import resolve_output, resolve_paths

if TYPE_CHECKING:
    from . import Repo

from . import locked


@locked
@scm_context
def imp_db(
    self: "Repo",
    url: str,
    target: str,
    type: str = "model",  # noqa: A002, pylint: disable=redefined-builtin
    out: Optional[str] = None,
    rev: Optional[str] = None,
    frozen: bool = True,
    force: bool = False,
    export_format: str = "csv",
):
    erepo = {"url": url}
    if rev:
        erepo["rev"] = rev

    assert type in ("model", "query")
    assert export_format in ("csv", "json")
    if not out:
        out = "results.csv" if type == "query" else f"{target}.{export_format}"

    db = {type: target, "export_format": export_format}
    out = resolve_output(url, out, force=force)
    path, wdir, out = resolve_paths(self, out, always_local=True)
    stage = self.stage.create(
        single_stage=True,
        validate=False,
        fname=path,
        wdir=wdir,
        deps=[url],
        outs=[out],
        erepo=erepo,
        fs_config=None,
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
