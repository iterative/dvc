from typing import TYPE_CHECKING

from scmrepo.exceptions import SCMError

from dvc.log import logger
from dvc.repo.experiments.executor.base import ExecutorInfo, TaskStatus
from dvc.repo.experiments.refs import EXEC_NAMESPACE, EXPS_NAMESPACE, EXPS_STASH
from dvc.repo.experiments.utils import get_exp_rwlock, iter_remote_refs

logger = logger.getChild(__name__)


if TYPE_CHECKING:
    from dvc.scm import Git

    from .base import BaseStashQueue


def get_remote_executor_refs(scm: "Git", remote_url: str) -> list[str]:
    """Get result list refs from a remote repository

    Args:
        remote_url : remote executor's url
    """
    refs = []
    for ref in iter_remote_refs(scm, remote_url, base=EXPS_NAMESPACE):
        if not ref.startswith(EXEC_NAMESPACE) and ref != EXPS_STASH:
            refs.append(ref)
    return refs


def fetch_running_exp_from_temp_dir(
    queue: "BaseStashQueue", rev: str, fetch_refs: bool
) -> dict[str, dict]:
    """Fetch status of running exps out of current working directory

    Args:
        queue (BaseStashQueue):
        rev (str): stash revision of the experiment
        fetch_refs (bool): fetch running checkpoint results to local or not.

    Returns:
        Dict[str, Dict]: _description_
    """
    from dvc.repo.experiments.executor.local import TempDirExecutor
    from dvc.scm import InvalidRemoteSCMRepo
    from dvc.utils.serialize import load_json

    result: dict[str, dict] = {}
    infofile = queue.get_infofile_path(rev)
    try:
        info = ExecutorInfo.from_dict(load_json(infofile))
    except OSError:
        return result
    if info.status <= TaskStatus.RUNNING:
        result[rev] = info.asdict()
        if info.git_url and fetch_refs and info.status > TaskStatus.PREPARING:

            def on_diverged(_ref: str):
                return True

            executor = TempDirExecutor.from_info(info)
            try:
                refs = get_remote_executor_refs(queue.scm, executor.git_url)
                with get_exp_rwlock(queue.repo, writes=refs):
                    for ref in executor.fetch_exps(
                        queue.scm,
                        refs,
                        on_diverged=on_diverged,
                    ):
                        logger.debug("Updated running experiment '%s'.", ref)
                        last_rev = queue.scm.get_ref(ref)
                        result[rev]["last"] = last_rev
                        if last_rev:
                            result[last_rev] = info.asdict()
            except (InvalidRemoteSCMRepo, SCMError):
                # ignore stale info files
                del result[rev]
    return result
