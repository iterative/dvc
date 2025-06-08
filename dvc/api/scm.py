from typing import Optional

from dvc.repo import Repo


def all_branches(repo: Optional[str] = None) -> list[str]:
    """Get all Git branches in a DVC repository.

    Args:
        repo (str, optional): location of the DVC repository.
            Defaults to the current project (found by walking up from the
            current working directory tree).
            It can be a URL or a file system path.
            Both HTTP and SSH protocols are supported for online Git repos
            (e.g. [user@]server:project.git).
    Returns:
        List[str]: Names of the Git branches.
    """
    with Repo.open(repo) as _repo:
        return _repo.scm.list_branches()


def all_commits(repo: Optional[str] = None) -> list[str]:
    """Get all Git commits in a DVC repository.

    Args:
        repo (str, optional): location of the DVC repository.
            Defaults to the current project (found by walking up from the
            current working directory tree).
            It can be a URL or a file system path.
            Both HTTP and SSH protocols are supported for online Git repos
            (e.g. [user@]server:project.git).
    Returns:
        List[str]: SHAs of the Git commits.
    """
    with Repo.open(repo) as _repo:
        return _repo.scm.list_all_commits()


def all_tags(repo: Optional[str] = None) -> list[str]:
    """Get all Git tags in a DVC repository.

    Args:
        repo (str, optional): location of the DVC repository.
            Defaults to the current project (found by walking up from the
            current working directory tree).
            It can be a URL or a file system path.
            Both HTTP and SSH protocols are supported for online Git repos
            (e.g. [user@]server:project.git).
    Returns:
        List[str]: Names of the Git tags.
    """
    with Repo.open(repo) as _repo:
        return _repo.scm.list_tags()
