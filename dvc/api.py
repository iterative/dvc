from contextlib import contextmanager
import os
import tempfile

try:
    from contextlib import _GeneratorContextManager as GCM
except ImportError:
    from contextlib import GeneratorContextManager as GCM

from dvc.utils import remove
from dvc.utils.compat import urlparse
from dvc.repo import Repo
from dvc.external_repo import ExternalRepo


def get_url(path, repo=None, remote=None):
    """Returns an url of a resource specified by path in repo"""
    with _make_repo(repo) as _repo:
        abspath = os.path.join(_repo.root_dir, path)
        out, = _repo.find_outs_by_path(abspath)
        remote_obj = _repo.cloud.get_remote(remote)
        return str(remote_obj.checksum_to_path_info(out.checksum))


def open(path, repo=None, remote=None, mode="r", encoding=None):
    """Opens a specified resource as a file descriptor"""
    args = (path,)
    kwargs = {
        "repo": repo,
        "remote": remote,
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
            "dvc.api.open() should be used in a with statement"
        )


def _open(path, repo=None, remote=None, mode="r", encoding=None):
    with _make_repo(repo) as _repo:
        abspath = os.path.join(_repo.root_dir, path)
        with _repo.open(
            abspath, remote=remote, mode=mode, encoding=encoding
        ) as fd:
            yield fd


def read(path, repo=None, remote=None, mode="r", encoding=None):
    """Read a specified resource into string"""
    with open(path, repo, remote=remote, mode=mode, encoding=encoding) as fd:
        return fd.read()


@contextmanager
def _make_repo(repo_url):
    if not repo_url or urlparse(repo_url).scheme == "":
        yield Repo(repo_url)
    else:
        tmp_dir = tempfile.mkdtemp("dvc-repo")
        try:
            ext_repo = ExternalRepo(tmp_dir, url=repo_url)
            ext_repo.install()
            yield ext_repo.repo
        finally:
            remove(tmp_dir)
