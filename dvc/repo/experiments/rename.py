import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from dvc.scm import Git

from .refs import ExpRefInfo
from .utils import exp_refs_by_rev

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.repo.experiments.queue.celery import LocalCeleryQueue

    from .queue.base import ExpRefAndQueueEntry


logger = logging.getLogger(__name__)


def rename(
    repo: "Repo",
    new_name: str,
    exp_name: Union[str, None] = None,
    rev: Optional[Union[List[str], str, None]] = None,
    git_remote: Optional[str] = None,
) -> List[str]:
    assert isinstance(repo.scm, Git)
    renamed: List[str] = []
    celery_queue: LocalCeleryQueue = repo.experiments.celery_queue

    if exp_name:
        results: Dict[
            str, ExpRefAndQueueEntry
        ] = celery_queue.get_ref_and_entry_by_names(exp_name, git_remote)
        for _, result in results.items():
            assert isinstance(result.exp_ref_info, ExpRefInfo)
            renamed_exp = _rename_exp(
                scm=repo.scm, ref_info=result.exp_ref_info, new_name=new_name
            )
            renamed.append(renamed_exp)
    elif rev:
        if isinstance(rev, list):
            rev = rev[0]
        ref_info_generator = exp_refs_by_rev(repo.scm, rev)
        for exp_ref_info in ref_info_generator:
            renamed_exp = _rename_exp(
                scm=repo.scm, ref_info=exp_ref_info, new_name=new_name
            )
            renamed.append(renamed_exp)

    return renamed


def _rename_exp(scm: "Git", ref_info: "ExpRefInfo", new_name: str):
    rev = scm.get_ref(str(ref_info))
    scm.remove_ref(str(ref_info))
    ref_info.name = new_name
    scm.set_ref(str(ref_info), rev)
    return new_name
