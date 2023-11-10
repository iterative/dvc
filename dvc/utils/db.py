import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, List, Optional, Sequence, Union

from dvc.exceptions import DvcException

if TYPE_CHECKING:
    from agate import Table
    from dbt.config.profile import Profile
    from dbt.contracts.results import RunResult


class DbtInternalError(DvcException):
    pass


def _ref(
    name: str,
    package: Optional[str] = None,
    version: Optional[int] = None,
) -> str:
    parts: List[str] = []
    if package:
        parts.append(repr(package))

    parts.append(repr(name))
    if version:
        parts.append(f"{version=}")

    inner = ",".join(parts)
    return "{{ ref(" + inner + ") }}"


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


def _dbt_show(
    inline: Optional[str] = None,
    limit: int = -1,
    profile: Optional[str] = None,
    target: Optional[str] = None,
) -> "Table":
    from dbt.contracts.results import RunExecutionResult

    result = _dbt_invoke(
        "show",
        inline=inline,
        limit=limit,
        profile=profile,
        target=target,
    )
    assert isinstance(result, RunExecutionResult)

    run_results: Sequence["RunResult"] = result.results
    run_result, *_ = run_results
    assert run_result.agate_table is not None
    return run_result.agate_table


def get_model(
    name: str,
    package: Optional[str] = None,
    version: Optional[int] = None,
    profile: Optional[str] = None,
    target: Optional[str] = None,
) -> "Table":
    model = _ref(name, package, version=version)
    q = f"select * from {model}"  # noqa: S608
    return _dbt_show(
        inline=q,
        profile=profile,
        target=target,
    )


def _profiles_dir(project_dir: Optional[str] = None) -> str:
    from dbt.cli.resolvers import default_profiles_dir

    if profiles_dir := os.getenv("DBT_PROFILES_DIR"):
        return profiles_dir
    if project_dir and os.path.isfile(os.path.join(project_dir, "profiles.yml")):
        return project_dir
    return os.fspath(default_profiles_dir())


@contextmanager
def _global_dbt_flags(
    profiles_dir: str,
    project_dir: str,
    target: Optional[str] = None,
):
    from argparse import Namespace

    from dbt import flags

    prev = flags.get_flags()
    try:
        args = Namespace(
            use_colors=True,
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target=target,
        )
        flags.set_from_args(args, None)
        yield flags.get_flags()
    finally:
        flags.set_flags(prev)


def _get_profile_or(
    project_dir: Optional[str], profile: Optional[str], target: Optional[str]
) -> "Profile":
    from dbt.config.profile import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.config.runtime import load_profile

    if project_dir and os.path.isfile(os.path.join(project_dir, "dbt_project.yml")):
        return load_profile(
            project_dir, {}, profile_name_override=profile, target_override=target
        )

    if not profile:
        raise DvcException("No profile specified to query from.")

    renderer = ProfileRenderer({})
    return Profile.render(renderer, profile, target_override=target)


def execute_sql(
    sql: str,
    profiles_dir: str,
    project_dir: Optional[str],
    profile: Optional[str],
    target: Optional[str] = None,
) -> "Table":
    from dbt.adapters import factory as adapters_factory
    from dbt.adapters.sql import SQLAdapter

    flags = _global_dbt_flags(profiles_dir, os.getcwd(), target=target)

    with flags, adapters_factory.adapter_management():
        profile_obj = _get_profile_or(project_dir, profile, target)
        adapters_factory.register_adapter(profile_obj)  # type: ignore[arg-type]
        adapter = adapters_factory.get_adapter(profile_obj)  # type: ignore[arg-type]

        assert isinstance(adapter, SQLAdapter)
        with adapter.connection_named("debug"):
            adapter.debug_query()

        with adapter.connection_named("execute"):
            exec_resp = adapter.execute(sql, fetch=True)
            _, table = exec_resp
            return table
