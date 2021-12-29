"""Manages source control systems (e.g. Git)."""
from contextlib import contextmanager
from functools import partial
from typing import TYPE_CHECKING, Iterator, List, Mapping, Optional

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


def SCM(
    root_dir, search_parent_directories=True, no_scm=False
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
        return Git(
            root_dir, search_parent_directories=search_parent_directories
        )


class TqdmGit(Tqdm):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("unit", "obj")
        super().__init__(*args, **kwargs)

    def update_git(self, event: "GitProgressEvent") -> None:
        phase, completed, total, message, *_ = event
        if phase:
            message = (phase + " | " + message) if message else phase
        if message:
            self.postfix["info"] = f" {message} | "
        if completed:
            self.update_to(completed, total)


def clone(url: str, to_path: str, **kwargs):
    from scmrepo.exceptions import CloneError as InternalCloneError

    with TqdmGit(desc="Cloning") as pbar:
        try:
            return Git.clone(url, to_path, progress=pbar.update_git, **kwargs)
        except InternalCloneError as exc:
            raise CloneError(str(exc))


def resolve_rev(scm: "Git", rev: str) -> str:
    from scmrepo.exceptions import RevError as InternalRevError

    from dvc.repo.experiments.utils import fix_exp_head

    try:
        return scm.resolve_rev(fix_exp_head(scm, rev))
    except InternalRevError as exc:
        # `scm` will only resolve git branch and tag names,
        # if rev is not a sha it may be an abbreviated experiment name
        if not rev.startswith("refs/"):
            from dvc.repo.experiments.utils import (
                AmbiguousExpRefInfo,
                resolve_name,
            )

            try:
                ref_infos = resolve_name(scm, rev).get(rev)
            except AmbiguousExpRefInfo:
                raise RevError(f"ambiguous Git revision '{rev}'")
            if ref_infos:
                return scm.get_ref(str(ref_infos))

        raise RevError(str(exc))


def iter_revs(
    scm: "Git",
    revs: Optional[List[str]] = None,
    num: int = 1,
    all_branches: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    all_experiments: bool = False,
) -> Mapping[str, List[str]]:

    if not any([revs, all_branches, all_tags, all_commits, all_experiments]):
        return {}

    revs = revs or []
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

    if all_commits:
        results.extend(scm.list_all_commits())
    else:
        if all_branches:
            results.extend(scm.list_branches())

        if all_tags:
            results.extend(scm.list_tags())

    if all_experiments:
        from dvc.repo.experiments.utils import exp_commits

        results.extend(exp_commits(scm))

    rev_resolver = partial(resolve_rev, scm)
    return group_by(rev_resolver, results)
