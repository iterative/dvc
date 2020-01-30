import os
from contextlib import contextmanager, _GeneratorContextManager as GCM

from dvc.repo import Repo
from dvc.exceptions import DvcException, NotDvcRepoError
from dvc.external_repo import external_repo


class UrlNotDvcRepoError(DvcException):
    """Thrown if given url is not a DVC repository.

    Args:
        url (str): URL to the repository
    """

    def __init__(self, url):
        super().__init__("'{}' is not a DVC repository.".format(url))


def get_url(path, repo=None, rev=None, remote=None):
    """
    Returns the full URL to the data artifact specified by its `path` in a
    `repo`.
    NOTE: There is no guarantee that the file actually exists in that location.
    """
    with _make_repo(repo, rev=rev) as _repo:
        _require_dvc(_repo)
        out = _repo.find_out_by_relpath(path)
        remote_obj = _repo.cloud.get_remote(remote)
        return str(remote_obj.checksum_to_path_info(out.checksum))


def open(path, repo=None, rev=None, remote=None, mode="r", encoding=None):
    """Context manager to open a file artifact as a file object."""
    args = (path,)
    kwargs = {
        "repo": repo,
        "remote": remote,
        "rev": rev,
        "mode": mode,
        "encoding": encoding,
    }
    return _OpenContextManager(_open, args, kwargs)


class _OpenContextManager(GCM):
    def __init__(self, func, args, kwds):
        self.gen = func(*args, **kwds)
        self.func, self.args, self.kwds = func, args, kwds

    def __getattr__(self, name):
        raise AttributeError(
            "dvc.api.open() should be used in a with statement."
        )


def _open(path, repo=None, rev=None, remote=None, mode="r", encoding=None):
    with _make_repo(repo, rev=rev) as _repo:
        with _repo.open_by_relpath(
            path, remote=remote, mode=mode, encoding=encoding
        ) as fd:
            yield fd


def read(path, repo=None, rev=None, remote=None, mode="r", encoding=None):
    """Returns the contents of a file artifact."""
    with open(
        path, repo=repo, rev=rev, remote=remote, mode=mode, encoding=encoding
    ) as fd:
        return fd.read()


@contextmanager
def _make_repo(repo_url=None, rev=None):
    repo_url = repo_url or os.getcwd()
    if rev is None and os.path.exists(repo_url):
        try:
            yield Repo(repo_url)
            return
        except NotDvcRepoError:
            pass  # fallthrough to external_repo
    with external_repo(url=repo_url, rev=rev) as repo:
        yield repo


def _require_dvc(repo):
    if not isinstance(repo, Repo):
        raise UrlNotDvcRepoError(repo.url)
