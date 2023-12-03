import os
from contextlib import contextmanager
from importlib.util import find_spec
from typing import TYPE_CHECKING, Optional

from funcy import cut_prefix

from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.utils import packaging

if TYPE_CHECKING:
    from dbt.config.profile import Profile

logger = logger.getChild(__name__)

DBT_PROJECT_FILE = "dbt_project.yml"


@contextmanager
def check_dbt(action: Optional[str] = None):
    if not (find_spec("dbt") and find_spec("dbt.cli")):
        action = f" {action}" if action else ""
        raise DvcException(f"Could not run{action}. dbt-core is not installed")
    yield packaging.check_required_version(pkg="dbt-core")


@contextmanager
def init_dbt(
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


def get_profiles_dir(project_dir: Optional[str] = None) -> str:
    from dbt.cli.resolvers import default_profiles_dir

    if profiles_dir := os.getenv("DBT_PROFILES_DIR"):
        return profiles_dir
    if project_dir and os.path.isfile(os.path.join(project_dir, "profiles.yml")):
        return project_dir
    return os.fspath(default_profiles_dir())


@contextmanager
def _handle_profile_parsing_error():
    from dbt.exceptions import DbtRuntimeError

    try:
        yield
    except DbtRuntimeError as e:
        cause = e.__cause__ is not None and e.__cause__.__context__
        if isinstance(cause, ModuleNotFoundError) and (
            adapter := cut_prefix(cause.name, "dbt.adapters.")
        ):
            # DbtRuntimeError is very noisy, so send it to debug
            logger.debug("", exc_info=True)
            raise DvcException(f"dbt-{adapter} dependency is missing") from cause
        raise DvcException("failed to read connection profiles") from e


def is_dbt_project(project_dir: str):
    return os.path.isfile(os.path.join(project_dir, DBT_PROJECT_FILE))


@_handle_profile_parsing_error()
def get_or_build_profile(
    project_dir: Optional[str], profile: Optional[str], target: Optional[str]
) -> "Profile":
    from dbt.config.profile import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.config.runtime import load_profile

    if project_dir and is_dbt_project(project_dir):
        return load_profile(
            project_dir, {}, profile_name_override=profile, target_override=target
        )

    if not profile:
        raise DvcException("No profile specified to query from.")

    renderer = ProfileRenderer({})
    return Profile.render(renderer, profile, target_override=target)
