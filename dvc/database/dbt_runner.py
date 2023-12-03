from typing import TYPE_CHECKING, List, Optional, Sequence, Union

from dvc.exceptions import DvcException
from dvc.log import logger

if TYPE_CHECKING:
    from agate import Table
    from dbt.contracts.results import RunResult

logger = logger.getChild(__name__)


class DbtInternalError(DvcException):
    pass


def _kw_to_cmd_args(**kwargs: Union[None, bool, str, int]) -> List[str]:
    args: List[str] = []
    for key, value in kwargs.items():
        key = key.replace("_", "-")
        if value is None:
            continue  # skip creating a flag in this case
        if value is True:
            args.append(f"--{key}")
        elif value is False:
            args.append(f"--no-{key}")
        else:
            args.extend([f"--{key}", str(value)])
    return args


def _dbt_invoke(*posargs: str, quiet: bool = True, **kw: Union[None, bool, str, int]):
    from dbt.cli.main import dbtRunner

    args = _kw_to_cmd_args(quiet=quiet or None)  # global options
    args.extend([*posargs, *_kw_to_cmd_args(**kw)])

    runner = dbtRunner()
    result = runner.invoke(args)
    if result.success:
        return result.result
    raise DbtInternalError(f"failed to run dbt {posargs[0]}") from result.exception


def dbt_show(
    inline: Optional[str] = None,
    limit: int = -1,
    profile: Optional[str] = None,
    target: Optional[str] = None,
) -> "Table":
    from dbt.contracts.results import RunExecutionResult

    result = _dbt_invoke(
        "show", inline=inline, limit=limit, profile=profile, target=target
    )
    assert isinstance(result, RunExecutionResult)

    run_results: Sequence["RunResult"] = result.results
    run_result, *_ = run_results
    assert run_result.agate_table is not None
    return run_result.agate_table
