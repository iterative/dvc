import logging
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from dvc.repo.experiments.exceptions import UnresolvedExpNamesError
from dvc.repo.experiments.utils import resolve_name
from dvc.scm import Git

from .refs import ExpRefInfo

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


def rename(
    repo: "Repo",
    new_name: str,
    exp_name: Union[str, None] = None,
    git_remote: Optional[str] = None,
) -> Union[List[str], None]:
    assert isinstance(repo.scm, Git)
    renamed: List[str] = []
    remained: List[str] = []

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
            assert isinstance(result, ExpRefInfo)
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
