import logging
from typing import TYPE_CHECKING, Dict

from scmrepo.exceptions import SCMError

from ..executor.base import ExecutorInfo, TaskStatus

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from .base import BaseStashQueue


def fetch_running_exp_from_temp_dir(
    queue: "BaseStashQueue", rev: str, fetch_refs: bool
) -> Dict[str, Dict]:
    """Fetch status of running exps out of current working directory

    Args:
        queue (BaseStashQueue):
        rev (str): stash revision of the experiment
        fetch_refs (bool): fetch running checkpoint results to local or not.

    Returns:
        Dict[str, Dict]: _description_
    """
    from dvc.scm import InvalidRemoteSCMRepo
    from dvc.utils.serialize import load_json

    from ..executor.local import TempDirExecutor

    result: Dict[str, Dict] = {}
    infofile = queue.get_infofile_path(rev)
    try:
        info = ExecutorInfo.from_dict(load_json(infofile))
    except OSError:
        return result
    if info.status < TaskStatus.FAILED:
        result[rev] = info.asdict()
        if info.git_url and fetch_refs and info.status > TaskStatus.PREPARING:

            def on_diverged(_ref: str, _checkpoint: bool):
                return False

            executor = TempDirExecutor.from_info(info)
            try:
                for ref in executor.fetch_exps(
                    queue.scm,
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
