from typing import TYPE_CHECKING, Any, Dict, Optional

from dvc.exceptions import OutputDuplicationError
from dvc.repo.scm_context import scm_context
from dvc.utils import resolve_output, resolve_paths

if TYPE_CHECKING:
    from . import Repo

from . import locked


@locked
@scm_context
def imp_db(  # noqa: PLR0913
    self: "Repo",
    url: Optional[str] = None,
    rev: Optional[str] = None,
    project_dir: Optional[str] = None,
    sql: Optional[str] = None,
    model: Optional[str] = None,
    version: Optional[int] = None,
    frozen: bool = True,
    profile: Optional[str] = None,
    target: Optional[str] = None,
    export_format: str = "csv",
    out: Optional[str] = None,
    force: bool = False,
):
    erepo = None
    if model and url:
        erepo = {"url": url}
        if rev:
            erepo["rev"] = rev

    assert export_format in ("csv", "json")

    db: Dict[str, Any] = {"export_format": export_format}
    if profile:
        db["profile"] = profile

    if model:
        out = out or f"{model}.{export_format}"
        db.update({"model": model, "version": version, "project_dir": project_dir})
    else:
        out = out or "results.csv"
        db["query"] = sql

    out = resolve_output(url or ".", out, force=force)
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

    stage.deps[0].target = target
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
