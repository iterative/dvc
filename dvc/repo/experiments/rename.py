from typing import TYPE_CHECKING, Dict, List, Optional, Union

from dvc.log import logger
from dvc.repo.experiments.exceptions import (
    ExperimentExistsError,
    UnresolvedExpNamesError,
)
from dvc.repo.experiments.utils import check_ref_format, resolve_name
from dvc.scm import Git

from .refs import ExpRefInfo

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logger.getChild(__name__)


def rename(
    repo: "Repo",
    new_name: str,
    exp_name: Union[str, None] = None,
    git_remote: Optional[str] = None,
    force: bool = False,
) -> Union[List[str], None]:
    renamed: List[str] = []
    remained: List[str] = []
    assert isinstance(repo.scm, Git)

    if exp_name == new_name:
        return None

    if exp_name:
        results: Dict[str, Union[ExpRefInfo, None]] = resolve_name(
            scm=repo.scm, exp_names=exp_name, git_remote=git_remote
        )
        for name, result in results.items():
            if result is None:
                remained.append(name)
                continue

            new_ref = ExpRefInfo(baseline_sha=result.baseline_sha, name=new_name)
            if repo.scm.get_ref(str(new_ref)) and not force:
                raise ExperimentExistsError(new_name)

            check_ref_format(repo.scm, new_ref)
            _rename_exp(scm=repo.scm, ref_info=result, new_name=new_name)
            renamed.append(name)

    if remained:
        raise UnresolvedExpNamesError(remained, git_remote=git_remote)

    return renamed


def _rename_exp(scm: "Git", ref_info: "ExpRefInfo", new_name: str):
    rev = scm.get_ref(str(ref_info))
    scm.remove_ref(str(ref_info))
    ref_info.name = new_name
    scm.set_ref(str(ref_info), rev)
    return new_name
