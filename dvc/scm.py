"""Manages source control systems (e.g. Git)."""
import os
from contextlib import contextmanager
from functools import partial
from typing import (
    TYPE_CHECKING,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Union,
    overload,
)

from funcy import group_by
from scmrepo.base import Base  # noqa: F401, pylint: disable=unused-import
from scmrepo.git import Git
from scmrepo.noscm import NoSCM

from dvc.exceptions import DvcException
from dvc.progress import Tqdm

if TYPE_CHECKING:
    from scmrepo.progress import GitProgressEvent


class SCMError(DvcException):
    """Base class for source control management errors."""


class CloneError(SCMError):
    pass


class RevError(SCMError):
    pass


class NoSCMError(SCMError):
    def __init__(self):
        msg = (
            "Only supported for Git repositories. If you're "
            "seeing this error in a Git repo, try updating the DVC "
            "configuration with `dvc config core.no_scm false`."
        )
        super().__init__(msg)


class InvalidRemoteSCMRepo(SCMError):
    pass


class GitAuthError(SCMError):
    def __init__(self, reason: str) -> None:
        doc = "See https://dvc.org/doc/user-guide/troubleshooting#git-auth"
        super().__init__(f"{reason}\n{doc}")


@contextmanager
def map_scm_exception(with_cause: bool = False) -> Iterator[None]:
    from scmrepo.exceptions import SCMError as InternalSCMError

    try:
        yield
    except InternalSCMError as exc:
        into = SCMError(str(exc))
        if with_cause:
            raise into from exc
        raise into


@overload
def SCM(
    root_dir: str,
    *,
    search_parent_directories: bool = ...,
    no_scm: Literal[False] = ...,
) -> "Git":
    ...


@overload
def SCM(
    root_dir: str,
    *,
    search_parent_directories: bool = ...,
    no_scm: Literal[True],
) -> "NoSCM":
    ...


@overload
def SCM(
    root_dir: str,
    *,
    search_parent_directories: bool = ...,
    no_scm: bool = ...,
) -> Union["Git", "NoSCM"]:
    ...


def SCM(
    root_dir, *, search_parent_directories=True, no_scm=False
):  # pylint: disable=invalid-name
    """Returns SCM instance that corresponds to a repo at the specified
    path.

    Args:
        root_dir (str): path to a root directory of the repo.
        search_parent_directories (bool): whether to look for repo root in
        parent directories.
        no_scm (bool): return NoSCM if True.

    Returns:
        dvc.scm.base.Base: SCM instance.
    """
    with map_scm_exception():
        if no_scm:
            return NoSCM(root_dir, _raise_not_implemented_as=NoSCMError)
        return Git(root_dir, search_parent_directories=search_parent_directories)


class TqdmGit(Tqdm):
    BAR_FMT = (
        "{desc}|{bar}|{postfix[info]}{n_fmt}/{total_fmt} [{elapsed}, {rate_fmt:>11}]"
    )

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("unit", "obj")
        kwargs.setdefault("bar_format", self.BAR_FMT)
        super().__init__(*args, **kwargs)
        self._last_phase = None

    def update_git(self, event: "GitProgressEvent") -> None:
        phase, completed, total, message, *_ = event
        if phase:
            message = (phase + " | " + message) if message else phase
        if message:
            self.set_msg(message)
        force_refresh = (  # force-refresh progress bar when:
            (total and completed and completed >= total)  # the task completes
            or total != self.total  # the total changes
            or phase != self._last_phase  # or, the phase changes
        )
        if completed is not None:
            self.update_to(completed, total)
        if force_refresh:
            self.refresh()
        self._last_phase = phase


def clone(url: str, to_path: str, **kwargs):
    from scmrepo.exceptions import CloneError as InternalCloneError

    from dvc.repo.experiments.utils import fetch_all_exps

    with TqdmGit(desc=f"Cloning {os.path.basename(url)}") as pbar:
        try:
            git = Git.clone(url, to_path, progress=pbar.update_git, **kwargs)
            if "shallow_branch" not in kwargs:
                fetch_all_exps(git, url, progress=pbar.update_git)
            return git
        except InternalCloneError as exc:
            raise CloneError("SCM error") from exc


def resolve_rev(scm: Union["Git", "NoSCM"], rev: str) -> str:
    from scmrepo.exceptions import RevError as InternalRevError

    from dvc.repo.experiments.utils import fix_exp_head

    try:
        return scm.resolve_rev(fix_exp_head(scm, rev))
    except InternalRevError as exc:
        assert isinstance(scm, Git)
        # `scm` will only resolve git branch and tag names,
        # if rev is not a sha it may be an abbreviated experiment name
        if not (rev == "HEAD" or rev.startswith("refs/")):
            from dvc.repo.experiments.utils import AmbiguousExpRefInfo, resolve_name

            try:
                ref_infos = resolve_name(scm, rev).get(rev)
            except AmbiguousExpRefInfo:
                raise RevError(f"ambiguous Git revision '{rev}'")  # noqa: B904
            if ref_infos:
                return scm.get_ref(str(ref_infos))

        raise RevError(str(exc))  # noqa: B904


def _get_n_commits(scm: "Git", revs: List[str], num: int) -> List[str]:
    results = []
    for rev in revs:
        if num == 0:
            continue
        results.append(rev)
        n = 1
        while True:
            if num == n:
                break
            try:
                head = f"{rev}~{n}"
                results.append(resolve_rev(scm, head))
            except RevError:
                break
            n += 1
    return results


def iter_revs(
    scm: "Git",
    revs: Optional[List[str]] = None,
    num: int = 1,
    all_branches: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    all_experiments: bool = False,
    commit_date: Optional[str] = None,
) -> Mapping[str, List[str]]:
    from scmrepo.exceptions import SCMError as _SCMError

    from dvc.repo.experiments.utils import exp_commits

    if not any(
        [
            revs,
            all_branches,
            all_tags,
            all_commits,
            all_experiments,
            commit_date,
        ]
    ):
        return {}

    revs = revs or []
    results: List[str] = _get_n_commits(scm, revs, num)

    if all_commits:
        results.extend(scm.list_all_commits())
    else:
        if all_branches:
            results.extend(scm.list_branches())

        if all_tags:
            results.extend(scm.list_tags())

        if commit_date:
            from datetime import datetime

            commit_datestamp = datetime.strptime(commit_date, "%Y-%m-%d").timestamp()

            def _time_filter(rev):
                try:
                    return scm.resolve_commit(rev).commit_time >= commit_datestamp
                except _SCMError:
                    return True

            results.extend(filter(_time_filter, scm.list_all_commits()))

    if all_experiments:
        results.extend(exp_commits(scm))

    rev_resolver = partial(resolve_rev, scm)
    return group_by(rev_resolver, results)
